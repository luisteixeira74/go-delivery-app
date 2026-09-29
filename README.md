# 🛵 Go Delivery - Processamento Concorrente de Pedidos & Rastreamento em Tempo Real

Aplicação desenvolvida em **Go (Golang)** simulando uma plataforma completa de entregas em tempo real com processamento assíncrono de pedidos via **Redis Queue**, atualização de posições de GPS e interface visual com **Kanban Kanban / WebSocket**.

---

## 📸 Painel de Operações

![Go Delivery Dashboard](./docs/images/dashboard.jpeg)

---

## 🎯 Principais Funcionalidades

- **Processamento Assíncrono via Redis:** Fila de mensagens (`RPush` / `BLPop`) para desalocar requisições HTTP do processamento pesado.
- **Worker Pool Concorrente:** Goroutines dedicadas consumindo a fila do Redis e executando simulações de transição de status em paralelo.
- **Atualização em Tempo Real (WebSocket / EventHub):** Comunicação bi-direcional para movimentação automática dos cards do Kanban e marcador de mapa.
- **Rastreamento de GPS:** Simulação de rota ponto a ponto do entregador enviando dados geográficos continuous via WebSocket.
- **Persistência de Dados:** Histórico e transições de pedidos gravados no **PostgreSQL**.

---

## 🛠️ Tecnologias Utilizadas

- **Linguagem:** Go 1.20+
- **Banco de Dados Relacional:** PostgreSQL
- **Fila & Cache:** Redis (`go-redis/v9`)
- **Comunicação em Tempo Real:** WebSockets (`gorilla/websocket`)
- **Frontend / Dashboard:** HTML5, CSS3, JavaScript (Leaflet.js para mapas)

---

## 🚀 Como Executar o Projeto

### 1. Pré-requisitos

Certifique-se de ter instalado em sua máquina:

- [Go](https://go.dev/) (v1.20 ou superior)
- [Docker](https://www.docker.com/) & Docker Compose

### 2. Subir os Serviços (PostgreSQL & Redis)

```bash
docker-compose up -d
```

### 🧪 Testando a Fila de Pedidos (Concorrência)

Para simular múltiplos pedidos simultâneos sendo enviados para a fila do Redis, execute o comando abaixo no terminal:

```bash
curl -X POST http://localhost:8080/orders \
 -H "Content-Type: application/json" \
 -d '{"store_id":"s1","delivery_latitude":-23.56168,"delivery_longitude":-46.655981,"items":[{"name":"X-Burguer","quantity":2,"price":29.0}]}' & \
curl -X POST http://localhost:8080/orders \
 -H "Content-Type: application/json" \
 -d '{"store_id":"s1","delivery_latitude":-23.56168,"delivery_longitude":-46.655981,"items":[{"name":"Pizza Pepperoni","quantity":1,"price":58.0}]}' &
```

20 Pedidos de uma vez

```bash
#!/bin/bash
echo "🚀 Disparando 20 pedidos para a API..."

for i in {1..20}
do
   curl -s -X POST http://localhost:8080/orders \
     -H "Content-Type: application/json" \
     -d '{
       "store_id": "11111111-1111-1111-1111-111111111111",
       "delivery_latitude": -23.56168,
       "delivery_longitude": -46.655981,
       "items": [{"name": "X-Burguer", "quantity": 1, "price": 25.0}]
     }' > /dev/null &
done

echo "✅ 20 pedidos enviados para a fila do Redis!"
```

### 📁 Estrutura de Diretórios

Plaintext

.
├── docs/
│ └── images/ # Imagens do sistema usadas na documentação
│ └── dashboard.png
├── events/ # Hub WebSocket e eventos de domínio
├── workers/ # Worker Pool & Consumidor da fila Redis
├── web/ # Arquivos estáticos e template do frontend
├── main.go # Ponto de entrada do servidor HTTP e conexões
└── README.md
