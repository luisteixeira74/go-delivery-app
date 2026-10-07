# Estágio 1: Build
FROM golang:alpine AS builder

# Habilita o download automático da toolchain caso o go.mod peça uma versão superior
ENV GOTOOLCHAIN=auto

WORKDIR /app

# Copia todo o código-fonte primeiro para evitar falhas de checksum entre go.mod e go.sum
COPY . .

# Baixa as dependências e compila os 3 utilitários
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/simulation ./cmd/simulation
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/seed ./cmd/seed

# ---------------------------------------------------
# Estágio 2: Imagem Final (Leve e Enxuta)
# ---------------------------------------------------
FROM alpine:latest
WORKDIR /app

# Instala certificados de segurança e tzdata para fuso horário
RUN apk --no-cache add ca-certificates tzdata

# Copia os binários compilados
COPY --from=builder /app/bin/server /app/server
COPY --from=builder /app/bin/simulation /app/simulation
COPY --from=builder /app/bin/seed /app/seed

# Copia arquivos estáticos do frontend (dashboard/módulos web)
COPY --from=builder /app/web /app/web

# Expõe as portas padrão da API e Gateway
EXPOSE 8080 8081

# Comando padrão caso não seja sobrescrito no docker-compose
CMD ["/app/server"]