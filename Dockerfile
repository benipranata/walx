FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/walx ./cmd/walx

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/walx /walx
ENTRYPOINT ["/walx"]
CMD ["-config", "/etc/walx/config.yaml"]
