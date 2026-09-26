package events

import "time"

// EventType define o tipo customizado para os tipos de evento
type EventType string

// Constantes com os tipos de eventos do sistema
const (
	OrderCreated        EventType = "ORDER_CREATED"
	OrderInPreparing    EventType = "ORDER_IN_PREPARATION"
	OrderReady          EventType = "ORDER_READY"
	OrderOutForDelivery EventType = "ORDER_OUT_FOR_DELIVERY" // Novo
	OrderDelivered      EventType = "ORDER_DELIVERED"        // Novo
	LocationUpdated     EventType = "LOCATION_UPDATED"
)

// ItemDTO representa os itens de um pedido
type ItemDTO struct {
    Name     string  `json:"name"`
    Quantity int     `json:"quantity"`
    Price    float64 `json:"price"`
}

// OrderEvent representa a estrutura de payload de um evento de pedido
type OrderEvent struct {
	EventID   string    `json:"event_id"`
	Type      EventType `json:"type"`
	OrderID   string    `json:"order_id"`
	StoreID   string    `json:"store_id"`
	WorkerID  int       `json:"worker_id,omitempty"`
	Items     []ItemDTO `json:"items,omitempty"`
	TotalCent int       `json:"total_cent,omitempty"`
	Latitude  float64   `json:"latitude,omitempty"`
	Longitude float64   `json:"longitude,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}