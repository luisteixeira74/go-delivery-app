package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"go-delivery/events" // Certifique-se de usar o nome correto do seu go.mod
	"go-delivery/workers"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type OrderItem struct {
	Name     string  `json:"name"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

type CreateOrderRequest struct {
	StoreID           string      `json:"store_id"`
	DeliveryLatitude  float64     `json:"delivery_latitude"`
	DeliveryLongitude float64     `json:"delivery_longitude"`
	Items             []OrderItem `json:"items"`
}

type OrderResponse struct {
	ID         string      `json:"id"`
	StoreID    string      `json:"store_id"`
	Status     string      `json:"status"`
	TotalCents int         `json:"total_cents"`
	Items      []OrderItem `json:"items"`
	CreatedAt  time.Time   `json:"created_at"`
}

type OrderTask struct {
	OrderID   string           `json:"order_id"`
	StoreID   string           `json:"store_id"`
	TotalCent int              `json:"total_cent"`
	Items     []events.ItemDTO `json:"items"`
}

var (
	db       *sql.DB
	eventHub *events.Hub
	workerPool *workers.Pool
)

func initDB() {
	var err error
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Erro ao conectar no banco de dados: %v", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("Erro ao validar conexão com PostgreSQL: %v", err)
	}

	fmt.Println("Conexão com PostgreSQL realizada com sucesso!")
}

func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
		return
	}

	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Items) == 0 {
		http.Error(w, "Payload inválido. Envie store_id, coordenadas e items.", http.StatusBadRequest)
		return
	}

	if req.StoreID == "" {
		req.StoreID = "11111111-1111-1111-1111-111111111111"
	}

	var totalCents int
	var itemsDTO []events.ItemDTO
	for _, item := range req.Items {
		totalCents += int(item.Price * 100) * item.Quantity
		itemsDTO = append(itemsDTO, events.ItemDTO{
			Name:     item.Name,
			Quantity: item.Quantity,
		})
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "Erro interno ao processar pedido", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	var orderID string
	status := "CONFIRMED"
	var createdAt time.Time

	queryOrder := `
		INSERT INTO orders (store_id, status, total_cents, delivery_latitude, delivery_longitude)
		VALUES ($1, $2::order_status, $3, $4, $5)
		RETURNING id, created_at
	`

	err = tx.QueryRow(
		queryOrder, 
		req.StoreID, 
		status, 
		totalCents, 
		req.DeliveryLatitude, 
		req.DeliveryLongitude,
	).Scan(&orderID, &createdAt)

	if err != nil {
		log.Printf("Erro ao inserir pedido: %v", err)
		http.Error(w, "Erro ao salvar pedido", http.StatusInternalServerError)
		return
	}

	queryItem := `
		INSERT INTO order_items (order_id, name, quantity, unit_price_cents)
		VALUES ($1, $2, $3, $4)
	`

	for _, item := range req.Items {
		itemPriceCents := int(item.Price * 100)

		if _, err := tx.Exec(
			queryItem,
			orderID,
			item.Name,
			item.Quantity,
			itemPriceCents,
		); err != nil {
			log.Printf("Erro ao inserir item: %v", err)
			http.Error(w, "Erro ao salvar itens do pedido", http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "Erro ao finalizar pedido", http.StatusInternalServerError)
		return
	}

	// Notifica via WebSocket para atualizar a coluna do Kanban
	eventHub.Publish(events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      events.OrderCreated,
		OrderID:   orderID,
		StoreID:   req.StoreID,
		WorkerID:  0, // 0 representa evento originado pela API HTTP
		Items:     itemsDTO,
		TotalCent: totalCents,
		Timestamp: time.Now(),
	})

	// Enfileira a tarefa no Worker Pool para movimentação de status
	workerPool.Enqueue(workers.OrderTask{
		OrderID:   orderID,
		StoreID:   req.StoreID,
		Items:     itemsDTO,
		TotalCent: totalCents,
	})

	resp := OrderResponse{
		ID:         orderID,
		StoreID:    req.StoreID,
		Status:     status,
		TotalCents: totalCents,
		Items:      req.Items,
		CreatedAt:  createdAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func createOrderHandler(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		// Cria dados do pedido
		task := OrderTask{
			OrderID:   uuid.NewString(),
			StoreID:   "store-01",
			TotalCent: 4500,
			Items: []events.ItemDTO{
				{Name: "X-Burguer", Quantity: 2},
				{Name: "Refrigerante", Quantity: 1},
			},
		}

		data, err := json.Marshal(task)
		if err != nil {
			http.Error(w, "Erro ao serializar pedido", http.StatusInternalServerError)
			return
		}

		// Envia para a fila do Redis (LPUSH)
		err = rdb.LPush(ctx, "queue:orders", data).Err()
		if err != nil {
			log.Printf("Erro ao publicar no Redis: %v", err)
			http.Error(w, "Erro ao enfileirar pedido", http.StatusInternalServerError)
			return
		}

		log.Printf("[API] 📥 Pedido %s enviado para a fila 'queue:orders'", task.OrderID)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"status":"queued","order_id":"%s"}`, task.OrderID)
	}
}

func main() {
	initDB()
	defer db.Close()

	// Inicializa e roda o Hub de eventos
	eventHub = events.NewHub()
	go eventHub.Run()

	// 2. Inicializa o Worker Pool com 3 workers concorrentes e buffer de 100 tarefas
	workerPool = workers.NewPool(3, 100, db, eventHub)
	workerPool.Start()

	// 1. Serve a página do Kanban na rota raiz /
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "web/index.html")
	})

	// Rota do WebSocket para o Kanban
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		events.ServeWs(eventHub, w, r)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "API e WebSocket operacionais!")
	})

	http.HandleFunc("/orders", createOrderHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Printf("Servidor rodando na porta %s (WebSocket em /ws)...\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}