FROM golang:1.25.4-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ENV CGO_ENABLED=0
RUN go build -trimpath -o /out/simulator ./cmd/simulator && \
    go build -trimpath -o /out/publisher ./cmd/publisher && \
    go build -trimpath -o /out/book-service ./cmd/book-service && \
    go build -trimpath -o /out/matching-service ./cmd/matching-service && \
    go build -trimpath -o /out/matching-workload ./cmd/matching-workload && \
    go build -trimpath -o /out/load-source ./cmd/load-source && \
    go build -trimpath -o /out/outcome-relay ./cmd/outcome-relay && \
    go build -trimpath -o /out/order-service ./cmd/order-service && \
    go build -trimpath -o /out/ticker-matcher ./cmd/ticker-matcher && \
    go build -trimpath -o /out/ticker-workload ./cmd/ticker-workload

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/ /app/
WORKDIR /app
USER 10001:10001
EXPOSE 19001/udp 19002 19003 19004 19005
ENTRYPOINT ["/app/book-service"]
