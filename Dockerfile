FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rss_sync .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/rss_sync /rss_sync
USER nonroot:nonroot
EXPOSE 8080 8081
ENTRYPOINT ["/rss_sync"]
