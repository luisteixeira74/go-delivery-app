package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
)

var ctx = context.Background()

// LocationPayload é a estrutura do evento transmitido via Redis Pub/Sub e WebSocket
type LocationPayload struct {
	Type      string  `json:"type"`
	OrderID   string  `json:"order_id"`
	CourierID string  `json:"courier_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// OrderStatusPayload transmite atualizações do estado do pedido
type OrderStatusPayload struct {
	Type    string `json:"type"`
	OrderID string `json:"order_id"`
}

func main() {
	// 1. Carrega variáveis de ambiente (.env)
	if err := godotenv.Load(); err != nil {
		log.Println("Aviso: .env não encontrado, usando padrões")
	}

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Erro ao conectar no Redis: %v", err)
	}
	log.Println("⚡ Worker de Simulação conectado ao Redis com sucesso!")

	// 2. Parâmetros do Pedido e Atores
	orderID := "33333333-3333-3333-3333-333333333333"
	courierID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

	// Coordenadas
	// Origem: Burger King - Pinheiros (-23.561414, -46.655881)
	startLat, startLng := -23.561414, -46.655881
	// Destino: Casa do Cliente / Paulista (-23.561680, -46.655981)
	endLat, endLng := -23.561680, -46.655981

	steps := 15             // Quantidade de passos no trajeto
	delay := 1500 * time.Millisecond // Intervalo entre cada atualização de GPS (1.5s)

	// 3. Notifica início do deslocamento (ORDER_OUT_FOR_DELIVERY)
	publishStatus(rdb, "ORDER_OUT_FOR_DELIVERY", orderID)

	log.Printf("🛵 Iniciando simulação de entrega para o Pedido %s...\n", orderID)

	// 4. Interpolação Linear e Atualização do GPS
	for i := 0; i <= steps; i++ {
		ratio := float64(i) / float64(steps)
		currLat := startLat + ratio*(endLat-startLat)
		currLng := startLng + ratio*(endLng-startLng)

		// 4a. Atualiza chave GEO no Redis para consultas por raio
		err := rdb.GeoAdd(ctx, "couriers:locations:sp", &redis.GeoLocation{
			Name:      courierID,
			Longitude: currLng,
			Latitude:  currLat,
		}).Err()
		if err != nil {
			log.Printf("Erro ao salvar GEO no Redis: %v", err)
		}

		// 4b. Prepara o payload para o canal Pub/Sub
		locEvent := LocationPayload{
			Type:      "LOCATION_UPDATED",
			OrderID:   orderID,
			CourierID: courierID,
			Latitude:  currLat,
			Longitude: currLng,
		}

		eventJSON, _ := json.Marshal(locEvent)

		// 4c. Publica no canal `courier:location:updated` que o WebSocket Hub escuta
		rdb.Publish(ctx, "courier:location:updated", eventJSON)

		log.Printf("📍 [%d/%d] GPS enviado: Lat %.6f | Lng %.6f\n", i, steps, currLat, currLng)

		time.Sleep(delay)
	}

	// 5. Finalização: Notifica conclusão da entrega (ORDER_DELIVERED e DELIVERED)
	log.Println("🎉 Entregador chegou ao destino!")
	publishStatus(rdb, "ORDER_DELIVERED", orderID)

	// Notificação direta para o app do entregador fechar a corrida
	deliveredEvent, _ := json.Marshal(map[string]string{
		"type":       "DELIVERED",
		"order_id":   orderID,
		"courier_id": courierID,
	})
	rdb.Publish(ctx, "courier:location:updated", deliveredEvent)
}

func publishStatus(rdb *redis.Client, statusType, orderID string) {
	payload := OrderStatusPayload{
		Type:    statusType,
		OrderID: orderID,
	}
	data, _ := json.Marshal(payload)
	rdb.Publish(ctx, "orders:status:updated", data)
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}