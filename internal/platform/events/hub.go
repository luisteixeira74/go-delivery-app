package events

import (
	"context"
	"encoding/json"
	"log"

	"github.com/redis/go-redis/v9"
)

type Hub struct {
	rdb         *redis.Client
	register    chan chan OrderEvent
	unregister  chan chan OrderEvent
	subscribers map[chan OrderEvent]bool
}

func NewHub(rdb *redis.Client) *Hub {
	return &Hub{
		rdb:         rdb,
		register:    make(chan chan OrderEvent),
		unregister:  make(chan chan OrderEvent),
		subscribers: make(map[chan OrderEvent]bool),
	}
}

// Publish publica o evento no Redis (alcança TODAS as instâncias da API)
func (h *Hub) Publish(ctx context.Context, event OrderEvent) {
	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("Erro ao serializar evento para Redis Pub/Sub: %v", err)
		return
	}

	// Publica no tópico global do Redis
	if err := h.rdb.Publish(ctx, "orders:events", payload).Err(); err != nil {
		log.Printf("Erro ao publicar evento no Redis: %v", err)
	}
}

func (h *Hub) Run(ctx context.Context) {
	// 1. Inscreve esta instância no tópico do Redis
	pubsub := h.rdb.Subscribe(ctx, "orders:events")
	defer pubsub.Close()

	redisChan := pubsub.Channel()

	for {
		select {
		// Gerencia registros de WebSockets locais
		case ch := <-h.register:
			h.subscribers[ch] = true
		case ch := <-h.unregister:
			if _, ok := h.subscribers[ch]; ok {
				delete(h.subscribers, ch)
				close(ch)
			}

		// 2. Evento chegou do Redis! Retransmite para os WebSockets conectados NESTA instância
		case msg := <-redisChan:
			var event OrderEvent
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				log.Printf("Erro ao deserializar evento do Redis: %v", err)
				continue
			}

			// Broadcast local
			for ch := range h.subscribers {
				select {
				case ch <- event:
				default:
					delete(h.subscribers, ch)
					close(ch)
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

// Subscribe cria um novo canal de OrderEvent, registra no Hub e o retorna.
func (h *Hub) Subscribe() chan OrderEvent {
	ch := make(chan OrderEvent, 256)
	h.register <- ch
	return ch
}

// Unsubscribe envia o canal para o Hub ser desregistrado e fechado.
func (h *Hub) Unsubscribe(ch chan OrderEvent) {
	h.unregister <- ch
}