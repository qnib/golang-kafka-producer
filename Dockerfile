FROM qnib/alplain-golang:edge as build

WORKDIR /usr/local/src/github.com/qnib/golang-kafka-producer
COPY main.go ./
COPY vendor ./vendor
RUN go build

FROM qnib/alplain-init:edge

RUN apk add --no-cache librdkafka
COPY --from=build /usr/local/src/github.com/qnib/golang-kafka-producer/golang-kafka-producer /usr/local/bin/kafka-producer
ENV KAFKA_BROKER=tasks.broker \
 KAFKA_TOPIC=test \
 MSG_DELAY_MS=300
ENTRYPOINT ["kafka-producer"]
