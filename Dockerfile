ARG BUILDPLATFORM=linux/amd64
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/rss_sync .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rss_sync /rss_sync
USER nonroot:nonroot
EXPOSE 8080 8081
ENTRYPOINT ["/rss_sync"]
