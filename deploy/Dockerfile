# One image, four entrypoints (reader, writer, vecwriter, api). Compose picks
# the binary per service with `command:`. Built from the repo root:
#   docker build -f deploy/Dockerfile -t bartie:local .
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/ ./cmd/...

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl && adduser -D -u 10001 bartie
COPY --from=build /out/reader /out/writer /out/api /out/cdcctl /usr/local/bin/
RUN mkdir -p /var/lib/bartie && chown bartie /var/lib/bartie
USER bartie
ENV MINICDC_ERROR_LOG=/var/lib/bartie/errors.jsonl
# Each service overrides this.
CMD ["api"]
