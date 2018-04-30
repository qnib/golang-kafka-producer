FROM qnib/alplain-golang:edge as build

WORKDIR /usr/local/src/github.com/qnib/golang-kafka-producer
COPY main.go ./
COPY vendor ./vendor
RUN go build

FROM qnib/alplain-init:edge

COPY --from=build /usr/local/src/github.com/qnib/golang-kafka-producer/golang-kafka-producer /usr/local/bin/kafka-producer
RUN apk add --no-cache librdkafka
ENV KAFKA_BROKER=tasks.broker
CMD ["kafka-producer"]