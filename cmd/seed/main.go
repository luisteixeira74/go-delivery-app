package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type StoreSeed struct {
	ID        string
	Name      string
	Address   string
	Latitude  float64
	Longitude float64
}

type CourierSeed struct {
	ID        string
	Name      string
	Vehicle   string
	Latitude  float64
	Longitude float64
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		dbHost = "localhost" // Fallback se rodar fora do Docker
	}

	dbPort := os.Getenv("DB_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}

	dbUser := os.Getenv("DB_USER")
	if dbUser == "" {
		dbUser = "postgres"
	}

	dbPassword := os.Getenv("DB_PASSWORD")
	if dbPassword == "" {
		dbPassword = "postgrespassword"
	}

	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "ifood_db"
	}

	pgConnStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPassword, dbHost, dbPort, dbName)
	db, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatalf("Erro ao conectar no Postgres: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Postgres indisponível: %v", err)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379" // Fallback seguro para rodar no Docker
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis indisponível: %v", err)
	}
	defer rdb.Close()

	fmt.Println("🚀 Iniciando execução do Seed...")

	stores := []StoreSeed{
		{
			ID:        "11111111-1111-1111-1111-111111111111",
			Name:      "Burger King - Pinheiros",
			Address:   "Av. Rebouças, 1000",
			Latitude:  -23.561680,
			Longitude: -46.655981,
		},
		{
			ID:        "22222222-2222-2222-2222-222222222222",
			Name:      "Pizza Hut - Faria Lima",
			Address:   "Av. Brig. Faria Lima, 2000",
			Latitude:  -23.568900,
			Longitude: -46.689000,
		},
	}

	couriers := []CourierSeed{
		{
			ID:        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			Name:      "Carlos Motoboy",
			Vehicle:   "MOTORCYCLE",
			Latitude:  -23.561000,
			Longitude: -46.655000,
		},
		{
			ID:        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
			Name:      "Ana Bike",
			Vehicle:   "BICYCLE",
			Latitude:  -23.567000,
			Longitude: -46.685000,
		},
	}

	fmt.Println("🧹 Limpando dados antigos...")
	_, _ = db.ExecContext(ctx, "TRUNCATE TABLE stores, couriers CASCADE;")
	rdb.Del(ctx, "couriers:locations:sp", "stores:locations:sp")

	fmt.Println("📦 Inserindo Lojas no Postgres e Redis...")
	for _, store := range stores {
		query := `
			INSERT INTO stores (id, name, address, latitude, longitude, created_at)
			VALUES ($1, $2, $3, $4, $5, NOW())
			ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, address = EXCLUDED.address;
		`
		_, err := db.ExecContext(ctx, query, store.ID, store.Name, store.Address, store.Latitude, store.Longitude)
		if err != nil {
			log.Fatalf("Erro ao inserir loja %s: %v", store.Name, err)
		}

		// Registra loja no Redis GEO para cálculo de Zona Morta
		err = rdb.GeoAdd(ctx, "stores:locations:sp", &redis.GeoLocation{
			Name:      store.ID,
			Longitude: store.Longitude,
			Latitude:  store.Latitude,
		}).Err()
		if err != nil {
			log.Fatalf("Erro ao cadastrar loja no Redis GEO: %v", err)
		}

		fmt.Printf("   ✓ Loja cadastrada: %s\n", store.Name)
	}

	fmt.Println("🛵 Inserindo Entregadores no Postgres e Redis...")
	for _, courier := range couriers {
		query := `
			INSERT INTO couriers (id, name, vehicle, status, created_at)
			VALUES ($1, $2, $3, 'IDLE', NOW())
			ON CONFLICT (id) DO UPDATE SET status = 'IDLE';
		`
		if _, err := db.ExecContext(ctx, query, courier.ID, courier.Name, courier.Vehicle); err != nil {
			log.Fatalf("Erro ao inserir entregador %s: %v", courier.Name, err)
		}

		err := rdb.GeoAdd(ctx, "couriers:locations:sp", &redis.GeoLocation{
			Name:      courier.ID,
			Longitude: courier.Longitude,
			Latitude:  courier.Latitude,
		}).Err()
		if err != nil {
			log.Fatalf("Erro ao adicionar local do entregador %s no Redis: %v", courier.Name, err)
		}

		statusKey := fmt.Sprintf("courier:%s:status", courier.ID)
		_ = rdb.Set(ctx, statusKey, "ONLINE", 0).Err()

		fmt.Printf("   ✓ Posição e Status registrados para %s (%f, %f)\n", courier.Name, courier.Latitude, courier.Longitude)
	}

	fmt.Println("\n✅ Seed executado com sucesso!")
}