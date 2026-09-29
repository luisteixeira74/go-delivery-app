package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-delivery/events"
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

var (
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

func initRedis(ctx context.Context) {
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

	tx, err := db.BeginTx(r.Context(), nil)
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

	err = tx.QueryRowContext(
		r.Context(),
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

		if _, err := tx.ExecContext(
			r.Context(),
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

	// 1. Notifica via WebSocket
	eventHub.Publish(events.OrderEvent{
		EventID:   uuid.NewString(),
		Type:      "ORDER_CREATED",
		OrderID:   orderID,
		StoreID:   req.StoreID,
		WorkerID:  0,
		Items:     itemsDTO,
		TotalCent: totalCents,
		Timestamp: time.Now(),
	})

	// 2. Enfileira no Redis
	taskPayload, err := json.Marshal(workers.OrderTask{
		OrderID:   orderID,
		StoreID:   req.StoreID,
		Items:     itemsDTO,
		TotalCent: totalCents,
	})
	if err != nil {
		log.Printf("Erro ao serializar OrderTask: %v", err)
	} else {
		if err := rdb.RPush(r.Context(), "order_tasks", taskPayload).Err(); err != nil {
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
	// 1. Cria contexto global vinculado a sinais do SO (Ctrl+C / SIGTERM)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	initDB()
	defer db.Close()

	initRedis(ctx)
	defer rdb.Close()

	// 2. Inicializa o Hub de WebSocket
	eventHub = events.NewHub()
	go eventHub.Run()

	// 3. Inicializa o Worker Pool (Limite de 3 workers concorrentes)
	workerPool := workers.NewWorkerPool(db, rdb, eventHub, 3)
	workerPool.Start(ctx)

	// 4. Mux e Rotas
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "web/index.html")
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		events.ServeWs(eventHub, w, r)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "API e WebSocket operacionais!")
	})
	mux.HandleFunc("/orders", createOrderHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// 5. Inicia o servidor em uma goroutine
	go func() {
		fmt.Printf("Servidor rodando na porta %s (WebSocket em /ws)...\n", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Erro ao iniciar servidor: %v", err)
		}
	}()

	// 6. Aguarda o sinal de encerramento (SIGINT/SIGTERM)
	<-ctx.Done()
	log.Println("\n🛑 Sinal de desligamento recebido. Iniciando Graceful Shutdown...")

	// Contexto com timeout de 5s para o encerramento das conexões pendentes do servidor HTTP
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Erro durante o desligamento do servidor HTTP: %v", err)
	} else {
		log.Println("✅ Servidor HTTP encerrado com sucesso.")
	}
}