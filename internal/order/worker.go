package order

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"go-delivery/internal/platform/events"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type OrderTask struct {
	OrderID   string           `json:"order_id"`
	StoreID   string           `json:"store_id"`
	Items     []events.ItemDTO `json:"items"`
	TotalCent int              `json:"total_cent"`
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

					w.processOrderIngestion(ctx, t)
				}(task)
			}
		}
	}()
}

// processOrderIngestion confirma o pedido no banco e notifica a cozinha via eventHub.
func (w *OrderWorker) processOrderIngestion(ctx context.Context, task OrderTask) {
	log.Printf("🟢 [ORDER WORKER] Registrando e validando pedido: %s", task.OrderID)

	// 1. Garante que o status no banco é CONFIRMED (compatível com o enum do Postgres)
	updateOrderStatus(w.db, w.eventHub, task.OrderID, "CONFIRMED", 0)

	// 2. Publica o evento usando a constante fortemente tipada events.OrderCreated
	w.eventHub.Publish(ctx, events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      events.OrderCreated, // Usando a constante do pacote events
		OrderID:   task.OrderID,
		StoreID:   task.StoreID,
		Items:     task.Items,
		TotalCent: task.TotalCent,
		Timestamp: time.Now(),
	})

	log.Printf("✅ [ORDER WORKER] Pedido %s pronto na fila do KDS. Aguardando ação da cozinha.", task.OrderID)
}

func updateOrderStatus(db *sql.DB, eventHub *events.Hub, orderID, status string, workerID int) {
	ctx := context.Background()

	// Atualiza no banco
	query := `UPDATE orders SET status = $1::order_status WHERE id = $2`
	if _, err := db.ExecContext(ctx, query, status, orderID); err != nil {
		log.Printf("Erro ao atualizar status do pedido %s: %v", orderID, err)
		return
	}

	// Notifica via WebSocket / EventHub
	eventType := "ORDER_" + status
	eventHub.Publish(ctx, events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      events.EventType(eventType),
		OrderID:   orderID,
		Status:    status,
		WorkerID:  workerID,
		Timestamp: time.Now(),
	})

	log.Printf("⚙️ Worker %d atualizou Pedido %s -> %s", workerID, orderID, status)
}

func StartOrderWorker(workerID int, rdb *redis.Client, db *sql.DB, eventHub *events.Hub) {
	log.Printf("👷 Worker %d iniciado e aguardando pedidos...", workerID)

	for {
		// BLPop bloqueia até que haja uma tarefa na fila "order_tasks"
		result, err := rdb.BLPop(context.Background(), 0, "order_tasks").Result()
		if err != nil {
			log.Printf("Erro no Worker %d ao consumir fila: %v", workerID, err)
			time.Sleep(1 * time.Second)
			continue
		}

		// result[0] é o nome da fila, result[1] é o payload JSON
		var task OrderTask
		if err := json.Unmarshal([]byte(result[1]), &task); err != nil {
			log.Printf("Worker %d: erro ao desserializar tarefa: %v", workerID, err)
			continue
		}

		log.Printf("⚡ Worker %d pegou o Pedido %s", workerID, task.OrderID)

		// 1. Simula tempo de preparação (ex: entre 3 e 6 segundos)
		prepTime := time.Duration(3+rand.Intn(4)) * time.Second
		time.Sleep(prepTime)

		updateOrderStatus(db, eventHub, task.OrderID, "IN_PREPARATION", workerID)

		// 2. Simula tempo finalizando a montagem (ex: entre 2 e 5 segundos)
		readyTime := time.Duration(2+rand.Intn(4)) * time.Second
		time.Sleep(readyTime)

		updateOrderStatus(db, eventHub, task.OrderID, "READY", workerID)
	}
}