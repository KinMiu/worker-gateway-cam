FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Compile binary untuk Linux
RUN CGO_ENABLED=0 GOOS=linux go build -o gateway cmd/gateway/main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/gateway .
EXPOSE 5093
CMD ["./gateway"]