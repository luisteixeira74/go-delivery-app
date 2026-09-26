FROM golang:alpine AS builder

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o main .

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/main .
# Copia a pasta web com o index.html para o container final
COPY --from=builder /app/web ./web

EXPOSE 8080

CMD ["./main"]