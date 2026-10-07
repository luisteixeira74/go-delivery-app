package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go-delivery/internal/delivery"
	"go-delivery/internal/order"
	"go-delivery/internal/platform/events"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type LocationPing struct {
	CourierID string  `json:"courier_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

var (
	db       *sql.DB
	rdb      *redis.Client
	eventHub *events.Hub
)

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func initDB() (*sql.DB, error) {
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "postgres")
	dbPassword := getEnv("DB_PASSWORD", "postgrespassword")
	dbName := getEnv("DB_NAME", "ifood_db")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	database, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("falha ao abrir driver PostgreSQL: %w", err)
	}

	database.SetMaxOpenConns(25)
	database.SetMaxIdleConns(5)
	database.SetConnMaxLifetime(5 * time.Minute)

	if err := database.Ping(); err != nil {
		database.Close()
		return nil, fmt.Errorf("falha ao validar PostgreSQL: %w", err)
	}

	log.Println("✅ Conexão com PostgreSQL realizada com sucesso!")
	return database, nil
}

func initRedis(ctx context.Context) *redis.Client {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Erro ao conectar no Redis em %s: %v", redisAddr, err)
	}

	log.Println("✅ Conexão com Redis realizada com sucesso!")
	return client
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("Aviso: Arquivo .env não encontrado, utilizando variáveis de ambiente do sistema")
	}

	modeFlag := flag.String("mode", "", "Modo de execução: api | gateway | dispatch | all")
	flag.Parse()

	mode := *modeFlag
	if mode == "" {
		mode = getEnv("APP_MODE", "all")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("⚙️  Inicializando Aplicação no Modo: [%s]", strings.ToUpper(mode))

	switch mode {
	case "api":
		runAPIService(ctx)
	case "gateway":
		runGatewayService(ctx)
	case "dispatch":
		runDispatchService(ctx)
	case "all":
		runMonolith(ctx)
	default:
		log.Fatalf("❌ Modo '%s' inválido. Escolha entre: api, gateway, dispatch ou all", mode)
	}
}

// -----------------------------------------------------------------------------
// 1. MÓDULO: API CORE & COZINHA (REST HTTP)
// -----------------------------------------------------------------------------
func runAPIService(ctx context.Context) {
    var err error
    db, err = initDB()
    if err != nil {
        log.Fatalf("Erro fatal no banco: %v", err)
    }
    defer db.Close()

    rdb = initRedis(ctx)
    defer rdb.Close()

    eventHub = events.NewHub(rdb)

    mux := http.NewServeMux()
    registerAPIRoutes(mux, db, rdb, eventHub)

    // FIX: Registrar arquivos estáticos da pasta web também na API Core
    fileServer := http.FileServer(http.Dir("./web"))
    mux.Handle("/", disableCacheMiddleware(fileServer))

    port := getEnv("PORT", "8080")
    startHTTPServer(ctx, mux, port, "API Core")
}

// -----------------------------------------------------------------------------
// 2. MÓDULO: GATEWAY (WEBSOCKETS & TELEMETRIA DE GPS)
// -----------------------------------------------------------------------------
func runGatewayService(ctx context.Context) {
	rdb = initRedis(ctx)
	defer rdb.Close()

	eventHub = events.NewHub(rdb)
	go eventHub.Run(ctx)

	mux := http.NewServeMux()

	// WebSocket Principal
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		events.ServeWs(eventHub, w, r)
	})

	// Ingestão de GPS em alta velocidade
	mux.HandleFunc("/couriers/location", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
			return
		}

		var ping LocationPing
		if err := json.NewDecoder(r.Body).Decode(&ping); err != nil {
			http.Error(w, "Payload inválido", http.StatusBadRequest)
			return
		}

		// 1. Atualiza Posição no Redis GEO
		err := rdb.GeoAdd(r.Context(), "couriers:locations:sp", &redis.GeoLocation{
			Name:      ping.CourierID,
			Longitude: ping.Longitude,
			Latitude:  ping.Latitude,
		}).Err()

		if err != nil {
			log.Printf("❌ Erro ao atualizar Redis GEO: %v", err)
			http.Error(w, "Erro ao gravar localização", http.StatusInternalServerError)
			return
		}

		// 2. Notifica o EventHub (Repassa via WebSocket para os mapas conectados)
		eventHub.Publish(r.Context(), events.OrderEvent{
			EventID:   ping.CourierID,
			Type:      "COURIER_LOCATION_UPDATED",
			OrderID:   "",
			Timestamp: time.Now(),
		})

		w.WriteHeader(http.StatusOK)
	})

	fileServer := http.FileServer(http.Dir("./web"))
	mux.Handle("/", disableCacheMiddleware(fileServer))

	port := getEnv("PORT", "8081")
	startHTTPServer(ctx, mux, port, "WebSocket Gateway")
}

// -----------------------------------------------------------------------------
// 3. MÓDULO: DISPATCH ENGINE (WORKER POOL E AGENTES AUTOMÁTICOS)
// -----------------------------------------------------------------------------
func runDispatchService(ctx context.Context) {
	var err error
	db, err = initDB()
	if err != nil {
		log.Fatalf("Erro fatal no banco: %v", err)
	}
	defer db.Close()

	rdb = initRedis(ctx)
	defer rdb.Close()

	eventHub = events.NewHub(rdb)
	go eventHub.Run(ctx)

	concurrencyLimit, _ := strconv.Atoi(getEnv("WORKER_CONCURRENCY", "3"))

	// Workers de Fila de Pedidos e Eventos Espaciais
	orderWorker := order.NewOrderWorker(db, rdb, eventHub, concurrencyLimit)
	orderWorker.Start(ctx)

	deliveryWorker := delivery.NewDeliveryWorker(db, eventHub, rdb)
	eventHandler := events.NewEventHandler(eventHub, deliveryWorker)
	go eventHandler.Start(ctx)

	log.Println("🛵 [Dispatch Engine] Workers rodando e aguardando chamados...")
	<-ctx.Done()
	log.Println("🛑 [Dispatch Engine] Encerrando Workers...")
}

// -----------------------------------------------------------------------------
// 4. MÓDULO: MONÓLITO (RODA TUDO NO MESMO PROCESSO)
// -----------------------------------------------------------------------------
func runMonolith(ctx context.Context) {
    // ... inicialização do DB, Redis, Workers ...

    mux := http.NewServeMux()

    // Rotas REST e WebSocket
    registerAPIRoutes(mux, db, rdb, eventHub)

    mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
        events.ServeWs(eventHub, w, r)
    })

    fileServer := http.FileServer(http.Dir("./web"))
    mux.Handle("/", disableCacheMiddleware(fileServer))

    // ENCAPSULE O MUX COM O MIDDLEWARE DE CORS
    handlerWithCORS := enableCORSMiddleware(mux)

    port := getEnv("PORT", "8080")
    startHTTPServer(ctx, handlerWithCORS, port, "Monólito Full")
}

// -----------------------------------------------------------------------------
// HELPER: REGISTRO DE ROTAS REST DA API
// -----------------------------------------------------------------------------
func registerAPIRoutes(mux *http.ServeMux, db *sql.DB, rdb *redis.Client, hub *events.Hub) {
	createHandler := order.CreateOrderHandler(db, hub, rdb)
	getHandler := order.GetOrdersHandler(db)
	updateStatusHandler := order.UpdateOrderStatusHandler(db, hub)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getHandler(w, r)
		case http.MethodPost:
			createHandler(w, r)
		default:
			http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/orders/", updateStatusHandler)
}

// -----------------------------------------------------------------------------
// HELPER: SERVIDOR HTTP E SHUTDOWN SUAVE
// -----------------------------------------------------------------------------
func startHTTPServer(ctx context.Context, handler http.Handler, port string, serviceName string) {
    server := &http.Server{
        Addr:    ":" + port,
        Handler: handler,
    }

	go func() {
		log.Printf("⚡ [%s] Servidor rodando em http://localhost:%s\n", serviceName, port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Erro no servidor HTTP [%s]: %v", serviceName, err)
		}
	}()

	<-ctx.Done()
	log.Printf("\n🛑 Sinal de desligamento recebido em [%s]. Encerrando...", serviceName)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("❌ Erro ao encerrar servidor [%s]: %v", serviceName, err)
	} else {
		log.Printf("✅ Servidor [%s] finalizado com sucesso.", serviceName)
	}
}

// Middleware para habilitar CORS
func enableCORSMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Define as permissões de acesso
        w.Header().Set("Access-Control-Allow-Origin", "*")
        w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

        // Trata requisições Preflight (OPTIONS) enviadas pelos navegadores
        if r.Method == http.MethodOptions {
            w.WriteHeader(http.StatusOK)
            return
        }

        next.ServeHTTP(w, r)
    })
}

func disableCacheMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		next.ServeHTTP(w, r)
	})
}