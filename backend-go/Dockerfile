FROM golang:1.21-alpine AS build
WORKDIR /src
COPY backend-go/go.mod backend-go/go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY backend-go ./
ARG TARGETOS=linux
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/scheduler ./cmd/scheduler && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/maintenance ./cmd/maintenance

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/api /usr/local/bin/api
COPY --from=build /out/worker /usr/local/bin/worker
COPY --from=build /out/scheduler /usr/local/bin/scheduler
COPY --from=build /out/maintenance /usr/local/bin/maintenance
ENTRYPOINT ["/usr/local/bin/api"]
