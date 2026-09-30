package delivery

import (
	"context"
	"database/sql"
	"log"
	"time"

	"go-delivery/internal/platform/events"

	"github.com/google/uuid"
)

type DeliveryWorker struct {
	db       *sql.DB
	eventHub *events.Hub
}

func NewDeliveryWorker(db *sql.DB, eventHub *events.Hub) *DeliveryWorker {
	return &DeliveryWorker{
		db:       db,
		eventHub: eventHub,
	}
}

// SimulateDelivery realiza a simulação do deslocamento por GPS até a entrega final
func (w *DeliveryWorker) SimulateDelivery(
	ctx context.Context,
	orderID string,
	storeID string,
	items []events.ItemDTO,
	totalCent int,
	startLat, startLng, endLat, endLng float64,
) {
	// Aguarda 10 segundos em READY (respeitando o cancelamento do contexto)
	select {
	case <-time.After(10 * time.Second):
	case <-ctx.Done():
		log.Printf("[GPS Worker] Simulação do pedido %s cancelada antes de iniciar.", orderID)
		return
	}

	// 1. Atualiza banco e avisa o Kanban: Saiu para entrega!
	w.updateStatus(orderID, "OUT_FOR_DELIVERY")
	w.eventHub.Publish(ctx, events.OrderEvent{
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
		// ✅ Substituído time.Sleep por select para reagir ao shutdown em qualquer passo
		select {
		case <-time.After(1500 * time.Millisecond):
		case <-ctx.Done():
			log.Printf("[GPS Worker] ⚠️ Interrompendo simulação GPS do pedido %s no passo %d/%d (Shutdown)", orderID, i, steps)
			return
		}

		currentLat := startLat + (endLat-startLat)*(float64(i)/float64(steps))
		currentLng := startLng + (endLng-startLng)*(float64(i)/float64(steps))

		w.eventHub.Publish(ctx, events.OrderEvent{
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
	w.updateStatus(orderID, "DELIVERED")
	w.eventHub.Publish(ctx, events.OrderEvent{
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

func (w *DeliveryWorker) updateStatus(orderID string, status string) {
	if w.db == nil {
		return
	}
	query := `UPDATE orders SET status = $1::order_status WHERE id = $2`
	if _, err := w.db.Exec(query, status, orderID); err != nil {
		log.Printf("Erro ao atualizar status para %s: %v", status, err)
	}
}