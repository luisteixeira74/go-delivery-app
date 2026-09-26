package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"go-delivery/events"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type OrderTask struct {
	OrderID   string           `json:"order_id"`
	StoreID   string           `json:"store_id"`
	TotalCent int              `json:"total_cent"`
	Items     []events.ItemDTO `json:"items"`
}

type WorkerPool struct {
	db       *sql.DB
	rdb      *redis.Client
	eventHub *events.Hub
	queueKey string
}

func NewWorkerPool(db *sql.DB, rdb *redis.Client, eventHub *events.Hub) *WorkerPool {
	return &WorkerPool{
		db:       db,
		rdb:      rdb,
		eventHub: eventHub,
		queueKey: "order_tasks",
	}
}

// Start inicia o consumidor de fila Redis em background
func (p *WorkerPool) Start(ctx context.Context) {
	log.Printf("[Worker Pool] 🚀 Consumidor da fila Redis iniciado (chave: '%s'). Aguardando pedidos...", p.queueKey)

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("[Worker Pool] 🛑 Encerrando consumidor da fila Redis...")
				return
			default:
				// BLPop bloqueia até 2 segundos esperando um item na chave "order_tasks"
				results, err := p.rdb.BLPop(ctx, 2*time.Second, p.queueKey).Result()
				if err != nil {
					if err == redis.Nil {
						continue // Fila temporariamente vazia
					}
					if ctx.Err() != nil {
						return
					}
					log.Printf("[Worker Pool] Erro ao ler fila do Redis: %v", err)
					time.Sleep(1 * time.Second)
					continue
				}

				var task OrderTask
				if err := json.Unmarshal([]byte(results[1]), &task); err != nil {
					log.Printf("[Worker Pool] Erro ao deserializar OrderTask: %v", err)
					continue
				}

				log.Printf("[Worker Pool] 📥 Pedido %s retirado da fila!", task.OrderID)

				go p.processOrderLifecycle(ctx, task)
			}
		}
	}()
}

func (p *WorkerPool) processOrderLifecycle(ctx context.Context, task OrderTask) {
	log.Printf("--------------------------------------------------")
	log.Printf("🟢 [WORKER] Iniciando ciclo do Pedido: %s", task.OrderID)
	log.Printf("--------------------------------------------------")

	// 1. Transição para IN_PREPARATION (3s)
	time.Sleep(3 * time.Second)
	p.updateOrderStatus(ctx, task.OrderID, "IN_PREPARATION")
	p.eventHub.Publish(events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      "ORDER_IN_PREPARATION",
		OrderID:   task.OrderID,
		StoreID:   task.StoreID,
		Items:     task.Items,
		TotalCent: task.TotalCent,
		Timestamp: time.Now(),
	})

	// 2. Transição para READY (4s)
	time.Sleep(4 * time.Second)
	p.updateOrderStatus(ctx, task.OrderID, "READY")
	p.eventHub.Publish(events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      "ORDER_READY",
		OrderID:   task.OrderID,
		StoreID:   task.StoreID,
		Items:     task.Items,
		TotalCent: task.TotalCent,
		Timestamp: time.Now(),
	})

	// 3. Simulação de Entrega e Trajeto GPS
	p.simulateDelivery(ctx, task)
}

func (p *WorkerPool) simulateDelivery(ctx context.Context, task OrderTask) {
	p.eventHub.Publish(events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      "ORDER_OUT_FOR_DELIVERY",
		OrderID:   task.OrderID,
		StoreID:   task.StoreID,
		Items:     task.Items,
		TotalCent: task.TotalCent,
		Timestamp: time.Now(),
	})

	startLat, startLng := -23.55052, -46.633308
	endLat, endLng := -23.56168, -46.655981

	steps := 5
	for i := 1; i <= steps; i++ {
		time.Sleep(2 * time.Second)

		currLat := startLat + (endLat-startLat)*(float64(i)/float64(steps))
		currLng := startLng + (endLng-startLng)*(float64(i)/float64(steps))

		p.eventHub.Publish(events.OrderEvent{
			EventID:   uuid.NewString(),
			Type:      "LOCATION_UPDATED",
			OrderID:   task.OrderID,
			StoreID:   task.StoreID,
			Latitude:  currLat,
			Longitude: currLng,
			Timestamp: time.Now(),
		})
	}

	// 4. Finalização: DELIVERED
	p.updateOrderStatus(ctx, task.OrderID, "DELIVERED")
	p.eventHub.Publish(events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      "ORDER_DELIVERED",
		OrderID:   task.OrderID,
		StoreID:   task.StoreID,
		Items:     task.Items,
		TotalCent: task.TotalCent,
		Timestamp: time.Now(),
	})

	log.Printf("🏁 [WORKER] Pedido %s finalizado com SUCESSO!", task.OrderID)
}

func (p *WorkerPool) updateOrderStatus(ctx context.Context, orderID string, status string) {
	if p.db == nil {
		return
	}
	query := `UPDATE orders SET status = $1::order_status WHERE id = $2`
	if _, err := p.db.ExecContext(ctx, query, status, orderID); err != nil {
		log.Printf("[Worker Pool] Erro ao atualizar status do pedido %s no PostgreSQL: %v", orderID, err)
	}
}