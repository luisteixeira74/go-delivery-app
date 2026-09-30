package events

import (
	"context"
	"log"
)

// DeliveryService define a interface que o handler precisa para acionar a entrega
type DeliveryService interface {
	SimulateDelivery(
		ctx context.Context,
		orderID string,
		storeID string,
		items []ItemDTO,
		totalCent int,
		startLat, startLng, endLat, endLng float64,
	)
}

type EventHandler struct {
	hub             *Hub
	deliveryService DeliveryService
}

func NewEventHandler(hub *Hub, deliveryService DeliveryService) *EventHandler {
	return &EventHandler{
		hub:             hub,
		deliveryService: deliveryService,
	}
}

// Start escuta o barramento de eventos e lida com cada um de forma reativa
func (h *EventHandler) Start(ctx context.Context) {
	log.Println("[Event Handler] 🎧 Escutando eventos do barramento central...")

	eventChan := h.hub.Subscribe()
	defer h.hub.Unsubscribe(eventChan)

	for {
		select {
		case <-ctx.Done():
			log.Println("[Event Handler] 🛑 Parando manipulador de eventos...")
			return

		case event, ok := <-eventChan:
			if !ok {
				log.Println("[Event Handler] ⚠️ Canal de eventos fechado. Encerrando...")
				return
			}
			// Propaga o ctx recebido do servidor
			h.routeEvent(ctx, event)
		}
	}
}

func (h *EventHandler) routeEvent(ctx context.Context, event OrderEvent) {
	switch event.Type {
	case OrderReady:
		log.Printf("[Event Handler] 📦 Pedido %s pronto. Acionando logística de entrega...", event.OrderID)

		startLat, startLng := -23.55052, -46.633308
		endLat, endLng := -23.56168, -46.655981

		if h.deliveryService != nil {
			// A goroutine herda o contexto global de encerramento do app
			go h.deliveryService.SimulateDelivery(
				ctx,
				event.OrderID,
				event.StoreID,
				event.Items,
				event.TotalCent,
				startLat, startLng, endLat, endLng,
			)
		}
	}
}