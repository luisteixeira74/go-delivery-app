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
	db        *sql.DB
	rdb       *redis.Client
	eventHub  *events.Hub
	queueKey  string
	semaphore chan struct{} // 👈 Semáforo para controle rigoroso de concorrência
}

func NewWorkerPool(db *sql.DB, rdb *redis.Client, eventHub *events.Hub, maxConcurrency int) *WorkerPool {
	return &WorkerPool{
		db:        db,
		rdb:       rdb,
		eventHub:  eventHub,
		queueKey:  "order_tasks",
		semaphore: make(chan struct{}, maxConcurrency), // Cria o buffer (ex: 3 slots)
	}
}

func (p *WorkerPool) Start(ctx context.Context) {
	log.Printf("[Worker Pool] 🚀 Consumidor Redis iniciado (Capacidade Limite: %d workers concorrentes)", cap(p.semaphore))

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("[Worker Pool] 🛑 Encerrando consumidor...")
				return
			default:
				// BLPop bloqueia aguardando tarefas na fila Redis
				results, err := p.rdb.BLPop(ctx, 2*time.Second, p.queueKey).Result()
				if err != nil {
					if err == redis.Nil {
						continue
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

				// 🔒 Adquire um slot no semáforo (Bloqueia se já houver 3 pedidos rodando)
				p.semaphore <- struct{}{}

				log.Printf("[Worker Pool] 📥 Pedido %s assumido por um Worker! (Slots ocupados: %d/%d)", 
					task.OrderID, len(p.semaphore), cap(p.semaphore))

				go func(t OrderTask) {
					defer func() {
						<-p.semaphore // 🔓 Libera o slot ao finalizar o ciclo do pedido
						log.Printf("[Worker Pool] 🔓 Slot liberado! (Slots em uso: %d/%d)", len(p.semaphore), cap(p.semaphore))
					}()

					p.processOrderLifecycle(ctx, t)
				}(task)
			}
		}
	}()
}

func (p *WorkerPool) processOrderLifecycle(ctx context.Context, task OrderTask) {
	log.Printf("🟢 [WORKER] Em execução: Pedido %s", task.OrderID)

	// 1. IN_PREPARATION (3s)
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

	// 2. READY (4s)
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

	// 3. Simulação GPS e Finalização
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

	steps := 3
	for i := 1; i <= steps; i++ {
		time.Sleep(1 * time.Second)
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

	log.Printf("🏁 [WORKER] Pedido %s CONCLUÍDO!", task.OrderID)
}

func (p *WorkerPool) updateOrderStatus(ctx context.Context, orderID string, status string) {
	if p.db == nil {
		return
	}
	query := `UPDATE orders SET status = $1::order_status WHERE id = $2`
	p.db.ExecContext(ctx, query, status, orderID)
}