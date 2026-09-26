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
	ctx      = context.Background()
	db       *sql.DB
	rdb      *redis.Client
	eventHub *events.Hub
)

func initDB() {
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Erro ao conectar no banco de dados: %v", err)
	}

	if err = db.Ping(); err != nil {
		log.Fatalf("Erro ao validar conexão com PostgreSQL: %v", err)
	}

	fmt.Println("Conexão com PostgreSQL realizada com sucesso!")
}

func initRedis() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Erro ao conectar no Redis: %v", err)
	}

	fmt.Println("Conexão com Redis realizada com sucesso!")
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

	// 1. Notifica via WebSocket para atualizar a UI/Kanban instantaneamente
	eventHub.Publish(events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      events.OrderCreated,
		OrderID:   orderID,
		StoreID:   req.StoreID,
		WorkerID:  0,
		Items:     itemsDTO,
		TotalCent: totalCents,
		Timestamp: time.Now(),
	})

	// 2. Publica a tarefa na fila do Redis (substituindo o WorkerPool local)
	taskPayload, err := json.Marshal(OrderTask{
		OrderID:   orderID,
		StoreID:   req.StoreID,
		Items:     itemsDTO,
		TotalCent: totalCents,
	})
	if err != nil {
		log.Printf("Erro ao serializar OrderTask: %v", err)
	} else {
		// Enfileira no Redis (Lista/Queue "order_tasks")
		if err := rdb.RPush(ctx, "order_tasks", taskPayload).Err(); err != nil {
			log.Printf("Erro ao enfileirar tarefa no Redis: %v", err)
		}
	}

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

func main() {
	initDB()
	defer db.Close()

	initRedis()
	defer rdb.Close()

	// Inicializa e roda o Hub de eventos
	eventHub = events.NewHub()
	go eventHub.Run()

	workerPool := workers.NewWorkerPool(db, rdb, eventHub)
	workerPool.Start(ctx)

	// Serve a página do Kanban na rota raiz /
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