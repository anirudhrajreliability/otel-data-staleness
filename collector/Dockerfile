# Build a custom OpenTelemetry Collector with the data-staleness components.
#   docker build -f collector/Dockerfile -t otelcol-datastaleness .
FROM golang:1.25 AS build
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.110.0
WORKDIR /src
COPY . .
RUN builder --config=collector/builder-config.yaml

FROM gcr.io/distroless/base-debian12
COPY --from=build /src/_build/otelcol-datastaleness /otelcol
ENTRYPOINT ["/otelcol"]
CMD ["--config=/etc/otelcol/config.yaml"]
