FROM hitachi.jfrog.io/docker/golang:1.21 as builder
ARG OTEL_VERSION=0.106.1
WORKDIR /
COPY . .
RUN go install go.opentelemetry.io/collector/cmd/builder@v${OTEL_VERSION}
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 builder --config builder-config.yaml
RUN mkdir -p /oteldata

FROM scratch
COPY --from=builder /otelcol-dev/otelcol-dev /otel
COPY --from=builder --chown=10001:10001 /oteldata /oteldata
USER 10001
ENTRYPOINT ["/otel"]
