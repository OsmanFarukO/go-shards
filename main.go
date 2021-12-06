package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	clientv3 "go.etcd.io/etcd/client/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	// "reflect"
)

var counterRoundRobin = 0
var selectedSearchServer string
var milvusSearchClient milvus.MilvusClient
var done = make(chan bool)
var etcdClient *clientv3.Client
var k8sClient typev1.CoreV1Interface

type OperationRequest struct {
	Type     string
	Id       int
	Encoding []float32
	Server   string
}

type SearchRequest struct {
	Encoding []float32
}

func getClient(configLocation string) (typev1.CoreV1Interface, error) {
	kubeconfig := filepath.Clean(configLocation)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset.CoreV1(), nil
}

func searchMilvusSvcs(ctx context.Context) {
	listOptions := metav1.ListOptions{}
	namespace := getEnv("NAMESPACE", "")
	svc_patt := getEnv("SVC_PATT", "")

	svcs, err := k8sClient.Services(namespace).List(ctx, listOptions)
	checkErr(err)

	serverList := []string{}

	for _, svc := range svcs.Items {
		if strings.Contains(svc.Name, svc_patt) {
			var svcWithNS = svc.Name + "." + svc.Namespace + ":19530"
			// health_check
			serverList = append(serverList, svcWithNS)
		}
	}

	mockExternalList := []string{"10.4.4.3:19530", "10.4.4.4:19530"}

	// this just mock service line
	_, err = etcdClient.Put(ctx, "/avaliable_servers", strings.Join(mockExternalList, ","))
	checkErr(err)
	done <- true
}

func startSearchingSvcs(ctx context.Context) {
	for {
		go searchMilvusSvcs(ctx)
		time.Sleep(60 * time.Second)
	}
}

func selectServerRR(ctx context.Context) {
	counterRoundRobin = counterRoundRobin + 1
	avaliable_server_list := getAvaliableServers(ctx)
	selectedSearchServer = avaliable_server_list[int(math.Mod(float64(counterRoundRobin), float64(len(avaliable_server_list))))]
	ipport := strings.Split(selectedSearchServer, ":")
	milvusSearchClient, _ = milvus.NewMilvusClient(ctx, milvus.ConnectParam{IPAddress: ipport[0], Port: ipport[1]})

	done <- true
}

func selectServer(ctx context.Context) {
	for {
		go selectServerRR(ctx)
		time.Sleep(5 * time.Second)
	}
}

func getAvaliableServers(ctx context.Context) []string {
	resp, err := etcdClient.Get(ctx, "/avaliable_servers")
	checkErr(err)
	return strings.Split(string(resp.Kvs[0].Value), ",")
}

func deleteOperationKey(ctx context.Context, opkey string) {
	// delete operation key here
	_, err := etcdClient.Delete(ctx, opkey, clientv3.WithPrefix())
	if err != nil {
		fmt.Println("Cant delete ", opkey, " from etcd")
	}
	keyList := getOperationsKeyList(ctx)
	newList := remove(keyList, opkey)
	_, puterr := etcdClient.Put(ctx, "/operation_keys", strings.Join(newList, ","))
	if puterr != nil {
		fmt.Println("Cant delete ", opkey, " from etcd operation_keys")
	}
}

func remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func insertToServers(ctx context.Context, operation OperationRequest, opKey string) {
	ipport := strings.Split(operation.Server, ":")
	milvusTmpClient, err := milvus.NewMilvusClient(ctx, milvus.ConnectParam{IPAddress: ipport[0], Port: ipport[1]})
	checkErr(err)

	records := make([]milvus.Entity, 1)
	records[0].FloatData = operation.Encoding
	insertParam := milvus.InsertParam{"encodings", "", records, []int64{int64(operation.Id)}}

	id_array, status, err := milvusTmpClient.Insert(ctx, &insertParam)
	if err != nil {
		fmt.Println("Insert rpc failed: " + err.Error())
	}
	if !status.Ok() {
		fmt.Println("Insert vector failed: " + status.GetMessage())
	} else {
		deleteOperationKey(ctx, opKey)
	}
}

func deleteFromServers(ctx context.Context, operation OperationRequest, opKey string) {
	ipport := strings.Split(operation.Server, ":")
	milvusTmpClient, err := milvus.NewMilvusClient(ctx, milvus.ConnectParam{IPAddress: ipport[0], Port: ipport[1]})
	checkErr(err)
	status, deleteerr := milvusTmpClient.DeleteEntityByID(ctx, "encodings", "", []int64{int64(operation.Id)})
	if deleteerr != nil {
		fmt.Println("delete operation failed operation key is ", opKey)
	}
	if !status.Ok() {
		fmt.Println("delete vector failed: " + status.GetMessage())
	} else {
		deleteOperationKey(ctx, opKey)
	}
	// func (client *Milvusclient) DeleteEntityByID(ctx context.Context, collectionName string, partitionTag string, id_array []int64) (Status, error)
}

func updateToServers(ctx context.Context, operation OperationRequest, opKey string) {
	// update encoding func
}

func checkEtcdOperations(ctx context.Context) {
	operationsKL := getOperationsKeyList(ctx)
	for i := 0; i < len(operationsKL); i++ {
		operation := getOperationInfo(ctx, operationsKL[i])
		if operation.Type == "i" {
			insertToServers(ctx, operation, operationsKL[i])
		} else if operation.Type == "d" {
			deleteFromServers(ctx, operation, operationsKL[i])
		} else if operation.Type == "u" {
			updateToServers(ctx, operation, operationsKL[i])
		}
	}
	done <- true
}

func getOperationInfo(ctx context.Context, opkey string) OperationRequest {
	operation, err := etcdClient.Get(ctx, "/"+opkey)
	checkErr(err)
	var op OperationRequest
	if len(operation.Kvs) > 0 {
		err := json.Unmarshal(operation.Kvs[0].Value, &op)
		checkErr(err)
	}
	return op
}

func checkNewOperations(ctx context.Context) {
	for {
		checkEtcdOperations(ctx)
		time.Sleep(60 * time.Second)
	}
}

func getOperationsKeyList(ctx context.Context) []string {
	kl, err := etcdClient.Get(ctx, "/operation_keys")
	checkErr(err)
	keyList := strings.Split(string(kl.Kvs[0].Value), ",")
	return keyList
}

func addKeyToOperationKeyList(ts string) {
	ctx := context.Background()
	opKeys := getOperationsKeyList(ctx)
	opKeys = append(opKeys, ts)
	_, err := etcdClient.Put(ctx, "/operation_keys", strings.Join(opKeys, ","))
	checkErr(err)
}

func addOperationsHandler(w http.ResponseWriter, r *http.Request) {
	// add operation as update insert delete to etcd server handler
	// curl localhost:8080/operations -H "Content-Type: application/json" -d '{"Type": "u/i/d", "Id": 0/1234, "Encoding": []/[512 dim]}'
	ctx := context.Background()
	var operator OperationRequest
	err := json.NewDecoder(r.Body).Decode(&operator)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	avaliable_servers := getAvaliableServers(ctx)
	if stringInSlice(operator.Type, []string{"i", "d", "u"}) {
		for i := 0; i < len(avaliable_servers); i++ {
			operationKey := strconv.FormatInt(time.Now().UTC().UnixNano(), 6)
			addKeyToOperationKeyList(operationKey)
			operator.Server = avaliable_servers[i]
			s, _ := json.Marshal(operator)
			_, err = etcdClient.Put(ctx, "/"+operationKey, string(s))
			checkErr(err)
		}
		go checkEtcdOperations(ctx)
		w.WriteHeader(200)
	} else {
		http.Error(w, "type is not valid use (u/i/d)", http.StatusBadRequest)
	}
}

func searchEncoding(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var searchop SearchRequest
	var topkQueryResult milvus.TopkQueryResult
	
	err := json.NewDecoder(r.Body).Decode(&searchop)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	extraParams := "{\"nprobe\" : 32}"
	q := make([]milvus.Entity, 1)
	q[0].FloatData = searchop.Encoding
	
	searchParam := milvus.SearchParam{"encodings", q, 2, nil, extraParams}
	topkQueryResult, _, err = milvusSearchClient.Search(ctx, searchParam)
	if err != nil {
		println("Search rpc failed: " + err.Error())
	}
	println("Search without index results: ")
	fmt.Println("Search : ", topkQueryResult)
	// for i := 0; i <= 1; i++ {
	// 	print(topkQueryResult.QueryResultList[i].Ids[0])
	// 	print("        ")
	// 	println(topkQueryResult.QueryResultList[i].Distances[0])
	// }
	w.WriteHeader(200)
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return defaultValue
	}
	return value
}

func main() {
	ctx := context.Background()

	etcdClient, _ = clientv3.New(clientv3.Config{
		Endpoints:   []string{"10.4.4.2:2379"},
		DialTimeout: 5 * time.Second,
	})

	kubeconfig := filepath.Join(
		getEnv("HOME", "~/"), ".kube", "config",
	)

	k8sClient, _ = getClient(kubeconfig)

	// check if not key provided
	_, _ = etcdClient.Put(ctx, "/operation_keys", "")

	go startSearchingSvcs(ctx)
	<-done
	go selectServer(ctx)
	<-done
	go checkNewOperations(ctx)
	<-done

	http.HandleFunc("/search", searchEncoding)
	http.HandleFunc("/operations", addOperationsHandler)
	http.ListenAndServe(":8080", nil)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
