FROM golang:1.17-alpine AS build_base

RUN apk add --no-cache git

WORKDIR /tmp/go-shard

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN go build -o ./out/go-shard .

FROM alpine:3.9 
RUN apk add ca-certificates

COPY --from=build_base /tmp/go-shard/.env.example /app/.env
COPY --from=build_base /tmp/go-shard/config /app/config
RUN source /app/.env
COPY --from=build_base /tmp/go-shard/out/go-shard /app/go-shard

EXPOSE 8080

CMD ["/app/go-shard"]