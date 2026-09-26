package workers

import (
	"database/sql"
	"log"
	"time"

	"go-delivery/events"

	"github.com/google/uuid"
)

// OrderTask representa a tarefa que o worker irá processar
type OrderTask struct {
	OrderID   string
	StoreID   string
	Items     []events.ItemDTO
	TotalCent int
}

// Pool gerencia a fila de jobs e o conjunto de goroutines trabalhadoras
type Pool struct {
	numWorkers int
	jobQueue   chan OrderTask
	db         *sql.DB
	hub        *events.Hub
}

func NewPool(numWorkers int, queueSize int, db *sql.DB, hub *events.Hub) *Pool {
	return &Pool{
		numWorkers: numWorkers,
		jobQueue:   make(chan OrderTask, queueSize),
		db:         db,
		hub:        hub,
	}
}

// Start inicia as N goroutines do worker pool em background
func (p *Pool) Start() {
	for i := 1; i <= p.numWorkers; i++ {
		go p.worker(i)
	}
	log.Printf("Worker Pool iniciado com %d workers ativos.", p.numWorkers)
}

// Enqueue adiciona um novo pedido na fila do Worker Pool
func (p *Pool) Enqueue(task OrderTask) {
	p.jobQueue <- task
}

// worker escuta o canal jobQueue e processa as transições de estado
func (p *Pool) worker(workerID int) {
	for task := range p.jobQueue {
		log.Printf("[Worker #%d] Assumiu o processamento do Pedido: %s", workerID, task.OrderID)

		// 1. Aguarda 5s e altera status para PREPARING
		time.Sleep(5 * time.Second)
		p.updateOrderStatus(task.OrderID, "PREPARING")

		p.hub.Publish(events.OrderEvent{
			EventID:   uuid.NewString(),
			Type:      events.OrderInPreparing,
			OrderID:   task.OrderID,
			StoreID:   task.StoreID,
			WorkerID:  workerID,
			Items:     task.Items,
			TotalCent: task.TotalCent,
			Timestamp: time.Now(),
		})
		log.Printf("[Worker #%d] Pedido %s -> IN_PREPARATION", workerID, task.OrderID)

		// 2. Aguarda 7s e altera status para READY (Pronto p/ Entrega)
		time.Sleep(7 * time.Second)
		p.updateOrderStatus(task.OrderID, "READY")

		p.hub.Publish(events.OrderEvent{
			EventID:   uuid.NewString(),
			Type:      events.OrderReady,
			OrderID:   task.OrderID,
			StoreID:   task.StoreID,
			WorkerID:  workerID,
			Items:     task.Items,
			TotalCent: task.TotalCent,
			Timestamp: time.Now(),
		})
		log.Printf("[Worker #%d] Pedido %s -> READY", workerID, task.OrderID)

		// 3. Dispara a simulação de entrega repassando os dados completos
		go SimulateDelivery(
			p.db,
			task.OrderID,
			task.StoreID,
			p.hub,
			task.Items,
			task.TotalCent,
			-23.58121265347526, -46.67692995986969, // Origem
			-23.58175054139284, -46.76376670589947, // Destino
		)
	}
}

// updateOrderStatus atualiza o status do pedido no banco PostgreSQL
func (p *Pool) updateOrderStatus(orderID string, status string) {
	query := `UPDATE orders SET status = $1::order_status WHERE id = $2`
	_, err := p.db.Exec(query, status, orderID)
	if err != nil {
		log.Printf("Erro ao atualizar status do pedido %s para %s: %v", orderID, status, err)
	}
}