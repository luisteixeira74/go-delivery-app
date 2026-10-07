package events

import "time"

// 1. Definição do tipo personalizado para EventType
type EventType string

const (
	OrderCreated        EventType = "ORDER_CREATED"
	OrderInPreparation  EventType = "ORDER_IN_PREPARATION"
	OrderReady          EventType = "ORDER_READY"
	OrderOutForDelivery EventType = "ORDER_OUT_FOR_DELIVERY"
	OrderDelivered      EventType = "ORDER_DELIVERED"
	LocationUpdated     EventType = "LOCATION_UPDATED"
)

type ItemDTO struct {
	Name     string  `json:"name"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

type OrderEvent struct {
	EventID   string    `json:"event_id"`
	Type      EventType `json:"type"`
	OrderID   string    `json:"order_id"`
	StoreID   string    `json:"store_id,omitempty"`
	Status    string    `json:"status,omitempty"`
	CourierID string    `json:"courier_id,omitempty"`
	WorkerID  int       `json:"worker_id,omitempty"`
	Items     []ItemDTO `json:"items,omitempty"`
	TotalCent int       `json:"total_cent,omitempty"`
	Latitude  float64   `json:"latitude,omitempty"`
	Longitude float64   `json:"longitude,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}