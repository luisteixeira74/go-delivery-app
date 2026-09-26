package events

import (
	"sync"
)

type Hub struct {
	// Canais registrados de clientes (WebSockets/SSE)
	clients map[chan OrderEvent]bool

	// Evento vindo dos Workers para ser transmitido
	broadcast chan OrderEvent

	// Registro/Remoção de clientes
	register   chan chan OrderEvent
	unregister chan chan OrderEvent

	mu sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[chan OrderEvent]bool),
		broadcast:  make(chan OrderEvent, 100),
		register:   make(chan chan OrderEvent),
		unregister: make(chan chan OrderEvent),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client)
			}
			h.mu.Unlock()

		case event := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client <- event:
				default:
					// Se o canal do cliente estiver cheio, ignora ou fecha para evitar blocking
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Publish é chamado pelos Workers quando o status do pedido muda
func (h *Hub) Publish(event OrderEvent) {
	h.broadcast <- event
}