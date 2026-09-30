package order

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"go-delivery/internal/platform/events"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type OrderTask struct {
	OrderID   string           `json:"order_id"`
	StoreID   string           `json:"store_id"`
	TotalCent int              `json:"total_cent"`
	Items     []events.ItemDTO `json:"items"`
}

type OrderWorker struct {
	db        *sql.DB
	rdb       *redis.Client
	eventHub  *events.Hub
	queueKey  string
	semaphore chan struct{}
}

func NewOrderWorker(db *sql.DB, rdb *redis.Client, eventHub *events.Hub, maxConcurrency int) *OrderWorker {
	return &OrderWorker{
		db:        db,
		rdb:       rdb,
		eventHub:  eventHub,
		queueKey:  "order_tasks",
		semaphore: make(chan struct{}, maxConcurrency),
	}
}

func (w *OrderWorker) Start(ctx context.Context) {
	log.Printf("[Order Worker] 🚀 Consumidor Redis iniciado (Capacidade Limite: %d workers concorrentes)", cap(w.semaphore))

	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("[Order Worker] 🛑 Encerrando consumidor...")
				return
			default:
				results, err := w.rdb.BLPop(ctx, 2*time.Second, w.queueKey).Result()
				if err != nil {
					if err == redis.Nil {
						continue
					}
					if ctx.Err() != nil {
						return
					}
					log.Printf("[Order Worker] Erro ao ler fila do Redis: %v", err)
					time.Sleep(1 * time.Second)
					continue
				}

				var task OrderTask
				if err := json.Unmarshal([]byte(results[1]), &task); err != nil {
					log.Printf("[Order Worker] Erro ao deserializar OrderTask: %v", err)
					continue
				}

				w.semaphore <- struct{}{}

				log.Printf("[Order Worker] 📥 Pedido %s assumido! (Slots ocupados: %d/%d)",
					task.OrderID, len(w.semaphore), cap(w.semaphore))

				go func(t OrderTask) {
					defer func() {
						<-w.semaphore
						log.Printf("[Order Worker] 🔓 Slot liberado! (Slots em uso: %d/%d)", len(w.semaphore), cap(w.semaphore))
					}()

					w.processOrderLifecycle(ctx, t)
				}(task)
			}
		}
	}()
}

func (w *OrderWorker) processOrderLifecycle(ctx context.Context, task OrderTask) {
	log.Printf("🟢 [ORDER WORKER] Em execução: Pedido %s", task.OrderID)

	// 1. IN_PREPARATION (3s)
	time.Sleep(3 * time.Second)
	w.updateOrderStatus(ctx, task.OrderID, "IN_PREPARATION")
	w.eventHub.Publish(ctx, events.OrderEvent{
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
	w.updateOrderStatus(ctx, task.OrderID, "READY")
	w.eventHub.Publish(ctx, events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      "ORDER_READY",
		OrderID:   task.OrderID,
		StoreID:   task.StoreID,
		Items:     task.Items,
		TotalCent: task.TotalCent,
		Timestamp: time.Now(),
	})
}

func (w *OrderWorker) updateOrderStatus(ctx context.Context, orderID string, status string) {
	if w.db == nil {
		return
	}
	query := `UPDATE orders SET status = $1::order_status WHERE id = $2`
	w.db.ExecContext(ctx, query, status, orderID)
}