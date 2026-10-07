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
	// Compara a conversão explícita de string para pegar qualquer variação do evento de pedido pronto
	switch string(event.Type) {
	case "ORDER_READY", "ORDER_READY_FOR_PICKUP", "READY", "READY_FOR_PICKUP":
		log.Printf("[Event Handler] 📦 Pedido %s pronto (%s). Acionando logística de entrega...", event.OrderID, event.Type)

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
		} else {
			log.Printf("[Event Handler] ⚠️ DeliveryService não foi configurado!")
		}

	default:
		log.Printf("[Event Handler] ℹ️ Evento recebido sem ação de entrega: %s", event.Type)
	}
}