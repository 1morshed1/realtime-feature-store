# Multi-stage build: compile all four service binaries, then ship them on a tiny base.
# One image contains every binary; docker-compose picks which to run via `command:`.

FROM golang:1.25 AS build
WORKDIR /src
# Cache modules first for faster rebuilds.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO disabled -> fully static binaries that run on a scratch/distroless base.
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /usr/local/bin/
# Default to the serving API; compose overrides `command` per service.
ENTRYPOINT ["/usr/local/bin/serving"]
