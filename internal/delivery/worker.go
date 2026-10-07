package delivery

import (
	"context"
	"database/sql"
	"log"
	"time"

	"go-delivery/internal/platform/events"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type DeliveryWorker struct {
	db       *sql.DB
	eventHub *events.Hub
	geo      *GeoService
}

func NewDeliveryWorker(db *sql.DB, eventHub *events.Hub, rdb *redis.Client) *DeliveryWorker {
	return &DeliveryWorker{
		db:       db,
		eventHub: eventHub,
		geo:      NewGeoService(rdb),
	}
}

// SimulateDelivery realiza o despacho dinâmico via Redis GEO e a simulação das duas fases do trajeto
func (w *DeliveryWorker) SimulateDelivery(
	ctx context.Context,
	orderID string,
	storeID string,
	items []events.ItemDTO,
	totalCent int,
	startLat, startLng, endLat, endLng float64,
) {
	// 1. Busca o entregador disponível mais próximo da loja (Raio de 5 km)
	courierID, cLat, cLng, err := w.geo.FindNearestCourier(ctx, startLat, startLng, 5.0)
	if err != nil {
		log.Printf("[GPS Worker] Pedido %s aguardando motoboy: %v", orderID, err)
		return
	}

	// 2. Transfere o entregador para status Ocupado (remove do GEO temporalmente)
	_ = w.geo.RemoveCourierAvailability(ctx, courierID)
	log.Printf("[GPS Worker] 🛵 Entregador %s alocado para o Pedido %s!", courierID, orderID)

	// --- FASE 1: Entregador desloca da Posição Atual -> Loja ---
	log.Printf("[GPS Worker] Pedido %s: Entregador a caminho da loja...", orderID)
	if !w.simulateRoute(ctx, orderID, storeID, cLat, cLng, startLat, startLng, 5) {
		return
	}

	// --- FASE 2: Saiu para Entrega (Loja -> Cliente) ---
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

	if !w.simulateRoute(ctx, orderID, storeID, startLat, startLng, endLat, endLng, 10) {
		return
	}

	// --- FASE 3: Pedido Entregue + Verificação de Zona Morta ---
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

	// Retorna o entregador para o pool na sua nova posição (local do cliente)
	_ = w.geo.UpdateCourierLocation(ctx, courierID, endLat, endLng)

	// Análise de Zona Morta (raio de 3 km do cliente sem lojas)
	isDeadZone, storesNearby, _ := w.geo.CheckDeadZone(ctx, endLat, endLng, 3.0)

	log.Printf("==================================================")
	log.Printf("🚩 [GPS Worker] PEDIDO %s FOI ENTREGUE COM SUCESSO!", orderID)
	if isDeadZone {
		log.Printf("⚠️ [ZONA MORTA] O local do cliente não possui lojas num raio de 3km! Sugerindo retorno do entregador ao centro comercial.")
	} else {
		log.Printf("📍 [LOGÍSTICA] Zona com alta demanda (%d lojas próximas). Entregador posicionado para novos chamados.", storesNearby)
	}
	log.Printf("==================================================")
}

func (w *DeliveryWorker) simulateRoute(
	ctx context.Context,
	orderID, storeID string,
	startLat, startLng, endLat, endLng float64,
	steps int,
) bool {
	for i := 1; i <= steps; i++ {
		select {
		case <-time.After(1500 * time.Millisecond):
		case <-ctx.Done():
			log.Printf("[GPS Worker] ⚠️ Interrompendo simulação GPS do pedido %s no passo %d/%d (Shutdown)", orderID, i, steps)
			return false
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
	return true
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