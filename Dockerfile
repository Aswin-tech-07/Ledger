# Build stage
FROM golang:1.22 AS builder
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build     go build -o /bin/api ./cmd/api &&     go build -o /bin/worker ./cmd/worker

# Run stage
FROM gcr.io/distroless/base-debian12
WORKDIR /
COPY --from=builder /bin/api /api
COPY --from=builder /bin/worker /worker
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/api"]
