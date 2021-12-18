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
	"strings"
	"time"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	uuid "github.com/nu7hatch/gouuid"
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
var instance_collection string

type OperationRequest struct {
	Type     string
	Id       int
	Encoding []float32
	Server   string
	State    int // 1 -> active 0 -> passive
}

type SearchRequest struct {
	Encoding [][]float32
}

var opts []clientv3.OpOption

// helper funcs

func remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
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
	namespace := getEnv("KUBE_NAMESPACE", "")
	svc_patt := getEnv("KUBE_SVC_PATT", "")

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
	_, delerr := etcdClient.Delete(ctx, opkey, clientv3.WithPrefix())
	if delerr != nil {
		fmt.Println("Cant delete ", opkey, " from etcd operation_keys")
	}
}

func insertToServers(ctx context.Context, operation OperationRequest, opKey string) {
	ipport := strings.Split(operation.Server, ":")
	milvusTmpClient, err := milvus.NewMilvusClient(ctx, milvus.ConnectParam{IPAddress: ipport[0], Port: ipport[1]})
	checkErr(err)

	records := make([]milvus.Entity, 1)
	records[0].FloatData = operation.Encoding
	insertParam := milvus.InsertParam{instance_collection, "", records, []int64{int64(operation.Id)}}

	_, status, err := milvusTmpClient.Insert(ctx, &insertParam)
	if err != nil {
		fmt.Println("Insert rpc failed: " + err.Error())
	}
	if !status.Ok() {
		fmt.Println("Insert vector failed: " + status.GetMessage())
	} else {
		// fmt.Println("progress ", opKey, " is finished deleting...")
		deleteOperationKey(ctx, opKey)
	}
}

func deleteFromServers(ctx context.Context, operation OperationRequest, opKey string) {
	ipport := strings.Split(operation.Server, ":")
	milvusTmpClient, err := milvus.NewMilvusClient(ctx, milvus.ConnectParam{IPAddress: ipport[0], Port: ipport[1]})
	checkErr(err)
	status, deleteerr := milvusTmpClient.DeleteEntityByID(ctx, instance_collection, "", []int64{int64(operation.Id)})
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
	ipport := strings.Split(operation.Server, ":")
	milvusTmpClient, err := milvus.NewMilvusClient(ctx, milvus.ConnectParam{IPAddress: ipport[0], Port: ipport[1]})
	checkErr(err)
	status, deleteerr := milvusTmpClient.DeleteEntityByID(ctx, instance_collection, "", []int64{int64(operation.Id)})
	if deleteerr != nil {
		fmt.Println("delete operation failed operation key is ", opKey)
	}
	if !status.Ok() {
		fmt.Println("delete vector failed: " + status.GetMessage())
	} else {
		records := make([]milvus.Entity, 1)
		records[0].FloatData = operation.Encoding
		insertParam := milvus.InsertParam{instance_collection, "", records, []int64{int64(operation.Id)}}
		_, _, inserterr := milvusTmpClient.Insert(ctx, &insertParam)
		if inserterr != nil {
			fmt.Println("update operation failed operation key is ", opKey)
		} else {
			deleteOperationKey(ctx, opKey)
		}
	}
}

func checkEtcdOperations(ctx context.Context) {
	operationsKL := getOperationsKeyList(ctx)
	for i := 0; i < len(operationsKL); i++ {
		operation := getOperationInfo(ctx, operationsKL[i])
		if operation.State == 1 {
			// fmt.Println("operation ", operationsKL[i], " in progress")
		} else {
			operation.State = 1
			s, _ := json.Marshal(operation)
			_, err := etcdClient.Put(ctx, operationsKL[i], string(s))
			if err != nil {
				fmt.Println("err")
			} else {
				if operation.Type == "i" {
					go insertToServers(ctx, operation, operationsKL[i])
				} else if operation.Type == "d" {
					go deleteFromServers(ctx, operation, operationsKL[i])
				} else if operation.Type == "u" {
					go updateToServers(ctx, operation, operationsKL[i])
				}
			}
		}
	}
	done <- true
}

func getOperationInfo(ctx context.Context, opkey string) OperationRequest {
	operation, err := etcdClient.Get(ctx, opkey)
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
		go checkEtcdOperations(ctx)
		time.Sleep(60 * time.Second)
	}
}

func getOperationsKeyList(ctx context.Context) []string {
	kl, err := etcdClient.Get(ctx, "/operation_keys", opts...)
	checkErr(err)
	keyList := []string{}
	if len(kl.Kvs) != 0 {
		for _, item := range kl.Kvs {
			keyList = append(keyList, string(item.Key))
		}
	}
	return keyList
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
	var operationKey *uuid.UUID
	if stringInSlice(operator.Type, []string{"i", "d", "u"}) {
		for i := 0; i < len(avaliable_servers); i++ {
			operationKey, _ = uuid.NewV4()
			operator.Server = avaliable_servers[i]
			operator.State = 0
			s, _ := json.Marshal(operator)
			_, err = etcdClient.Put(ctx, "/operation_keys/"+operationKey.String(), string(s))
			checkErr(err)
		}
		go checkEtcdOperations(ctx)
		w.WriteHeader(200)
	} else {
		http.Error(w, "type is not valid use (u/i/d)", http.StatusBadRequest)
	}
}

func searchEncoding(w http.ResponseWriter, r *http.Request) {
	// curl -X POST localhost:8080/search -H 'Content-Type: application/json' -d '{"Encoding":[[],[] ... ]}'
	ctx := context.Background()
	var searchop SearchRequest
	var topkQueryResult milvus.TopkQueryResult

	err := json.NewDecoder(r.Body).Decode(&searchop)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	extraParams := "{\"nprobe\" : " + getEnv("SEARCH_NPROBE", "32") + "}"
	q := make([]milvus.Entity, len(searchop.Encoding))
	for enc := 0; enc < len(searchop.Encoding); enc++ {
		q[enc].FloatData = searchop.Encoding[enc]
	}

	searchParam := milvus.SearchParam{instance_collection, q, 2, nil, extraParams}
	topkQueryResult, _, err = milvusSearchClient.Search(ctx, searchParam)
	if err != nil {
		println("Search rpc failed: " + err.Error())
	}
	fmt.Println("Search : ", topkQueryResult)
	jsonresp, _ := json.Marshal(topkQueryResult)
	w.Write(jsonresp)
	w.WriteHeader(200)
}

func main() {
	ctx := context.Background()

	etcdClient, _ = clientv3.New(clientv3.Config{
		Endpoints:   []string{getEnv("ETCD_HOST", "10.4.4.2") + ":" + 
							  getEnv("ETCD_PORT", "2379")}, // "10.4.4.2:2379"
		DialTimeout: 5 * time.Second,
	})

	opts = []clientv3.OpOption{
		clientv3.WithPrefix(),
	}

	kubeconfig := filepath.Join(
		getEnv("KUBE_DIR", "/home/osman"), ".kube", "config",
	)

	k8sClient, _ = getClient(kubeconfig)

	instance_collection = getEnv("MILVUS_COLLECTION", "encodings")

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
