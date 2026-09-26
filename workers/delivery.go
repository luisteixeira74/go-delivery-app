package workers

import (
	"database/sql"
	"log"
	"time"

	"go-delivery/events"

	"github.com/google/uuid"
)

func SimulateDelivery(
	db *sql.DB,
	orderID string,
	storeID string,
	hub *events.Hub,
	items []events.ItemDTO,
	totalCent int,
	startLat, startLng, endLat, endLng float64,
) {
	// Aguarda 10 segundos em READY para dar tempo de visualizar o card na coluna "Prontos"
	time.Sleep(10 * time.Second)

	// 1. Atualiza banco e avisa o Kanban: Saiu para entrega!
	updateStatus(db, orderID, "OUT_FOR_DELIVERY")
	hub.Publish(events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      events.OrderOutForDelivery,
		OrderID:   orderID,
		StoreID:   storeID,
		Items:     items,
		TotalCent: totalCent,
		Timestamp: time.Now(),
	})
	log.Printf("[GPS Worker] Pedido %s -> OUT_FOR_DELIVERY", orderID)

	steps := 10
	for i := 1; i <= steps; i++ {
		time.Sleep(1500 * time.Millisecond)

		currentLat := startLat + (endLat-startLat)*(float64(i)/float64(steps))
		currentLng := startLng + (endLng-startLng)*(float64(i)/float64(steps))

		hub.Publish(events.OrderEvent{
			EventID:   uuid.NewString(),
			Type:      events.LocationUpdated,
			OrderID:   orderID,
			StoreID:   storeID,
			Timestamp: time.Now(),
			Latitude:  currentLat,
			Longitude: currentLng,
		})

		log.Printf("[GPS Worker] Pedido %s -> Passo %d/%d (Lat: %.5f, Lng: %.5f)", orderID, i, steps, currentLat, currentLng)
	}

	// 2. Chegou ao destino: Atualiza banco e avisa o Kanban!
	updateStatus(db, orderID, "DELIVERED")
	hub.Publish(events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      events.OrderDelivered,
		OrderID:   orderID,
		StoreID:   storeID,
		Items:     items,
		TotalCent: totalCent,
		Timestamp: time.Now(),
	})
	log.Printf("==================================================")
	log.Printf("🚩 [GPS Worker] PEDIDO %s FOI ENTREGUE COM SUCESSO!", orderID)
	log.Printf("==================================================")
}

func updateStatus(db *sql.DB, orderID string, status string) {
	query := `UPDATE orders SET status = $1::order_status WHERE id = $2`
	if _, err := db.Exec(query, status, orderID); err != nil {
		log.Printf("Erro ao atualizar status para %s: %v", status, err)
	}
}