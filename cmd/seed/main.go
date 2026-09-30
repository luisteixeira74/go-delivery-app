package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq" // Driver do Postgres
	"github.com/redis/go-redis/v9"
)

// Estruturas de dados para o Seed
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

	pgConnStr := "postgres://postgres:postgrespassword@localhost:5432/ifood_db?sslmode=disable"
	db, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatalf("Erro ao conectar no Postgres: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Postgres indisponível: %v", err)
	}

	// 2. Conexão com Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Redis indisponível: %v", err)
	}
	defer rdb.Close()

	fmt.Println("🚀 Iniciando execução do Seed...")

	// ---------------------------------------------------------
	// DADOS DE TESTE
	// ---------------------------------------------------------
	stores := []StoreSeed{
		{
			ID:        "11111111-1111-1111-1111-111111111111",
			Name:      "Burger King - Pinheiros",
			Latitude:  -23.561680,
			Longitude: -46.655981,
		},
		{
			ID:        "22222222-2222-2222-2222-222222222222",
			Name:      "Pizza Hut - Faria Lima",
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

	// ---------------------------------------------------------
	// PASSO 1: LIMPEZA (OPCIONAL, BOM PARA TESTES REPETÍVEIS)
	// ---------------------------------------------------------
	fmt.Println("🧹 Limpando dados antigos...")
	_, _ = db.ExecContext(ctx, "TRUNCATE TABLE stores, couriers CASCADE;")
	rdb.Del(ctx, "couriers:locations:sp")

	// ---------------------------------------------------------
	// PASSO 2: POPULAR POSTGRES
	// ---------------------------------------------------------
	fmt.Println("📦 Inserindo Lojas no Postgres...")
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
			fmt.Printf("   ✓ Loja cadastrada: %s\n", store.Name)
	}

	fmt.Println("🛵 Inserindo Entregadores no Postgres...")
	for _, courier := range couriers {
		query := `
			INSERT INTO couriers (id, name, vehicle, status, created_at)
			VALUES ($1, $2, $3, 'IDLE', NOW())
			ON CONFLICT (id) DO UPDATE SET status = 'IDLE';
		`
		_, err := db.ExecContext(ctx, query, courier.ID, courier.Name, courier.Vehicle)
		if err != nil {
			log.Fatalf("Erro ao inserir entregador %s: %v", courier.Name, err)
		}
		fmt.Printf("   ✓ Entregador cadastrado: %s\n", courier.Name)
	}

	// ---------------------------------------------------------
	// PASSO 3: POPULAR REDIS SPATIAL (GEOADD)
	// ---------------------------------------------------------
	fmt.Println("📍 Registrando localização e marcando entregadores ONLINE no Redis...")
	for _, courier := range couriers {
		// A. Registra a Posição Geográfica para a Busca de Raio (GEOADD)
		// Nota: No Redis, a ordem é Longitude (X), Latitude (Y)
		err := rdb.GeoAdd(ctx, "couriers:locations:sp", &redis.GeoLocation{
			Name:      courier.ID,
			Longitude: courier.Longitude,
			Latitude:  courier.Latitude,
		}).Err()

		if err != nil {
			log.Fatalf("Erro ao adicionar local do entregador %s no Redis: %v", courier.Name, err)
		}

		// B. Registra o Status do Entregador como ONLINE
		statusKey := fmt.Sprintf("courier:%s:status", courier.ID)
		err = rdb.Set(ctx, statusKey, "ONLINE", 0).Err()
		if err != nil {
			log.Fatalf("Erro ao setar status ONLINE do entregador %s no Redis: %v", courier.Name, err)
		}

		fmt.Printf("   ✓ Posição e Status registados para %s (%f, %f)\n", courier.Name, courier.Latitude, courier.Longitude)
	}

	fmt.Println("\n✅ Seed executado com sucesso! Agora você pode rodar simulações com os IDs estáticos.")
}