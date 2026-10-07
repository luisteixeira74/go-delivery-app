package order

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"go-delivery/internal/platform/events"
)

// --- STRUCTS DTOs & REQUESTS/RESPONSES ---

type CreateOrderRequest struct {
	StoreID           string           `json:"store_id"`
	DeliveryLatitude  float64          `json:"delivery_latitude"`
	DeliveryLongitude float64          `json:"delivery_longitude"`
	Items             []CreateItemItem `json:"items"`
}

type CreateItemItem struct {
	Name     string  `json:"name"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

type OrderResponse struct {
	ID         string           `json:"id"`
	StoreID    string           `json:"store_id"`
	Status     string           `json:"status"`
	TotalCents int              `json:"total_cents"`
	Items      []CreateItemItem `json:"items"`
	CreatedAt  time.Time        `json:"created_at"`
}

type OrderDTO struct {
	ID         string    `json:"id"`
	StoreID    string    `json:"store_id"`
	Status     string    `json:"status"`
	TotalCents int       `json:"total_cents"`
	CreatedAt  time.Time `json:"created_at"`
	Items      []ItemDTO `json:"items"`
}

type ItemDTO struct {
	Name     string  `json:"name"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

type UpdateStatusRequest struct {
	Status string `json:"status"`
}

// --- HANDLERS HTTP ---

// CreateOrderHandler processa a criação de um novo pedido, grava no Postgres,
// notifica o barramento de eventos e enfileira no Redis.
func CreateOrderHandler(db *sql.DB, eventHub *events.Hub, rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
			itemPriceCents := int(item.Price * 100)
			totalCents += itemPriceCents * item.Quantity

			itemsDTO = append(itemsDTO, events.ItemDTO{
				Name:     item.Name,
				Quantity: item.Quantity,
				Price:    item.Price * 100,
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

		// Grava usando a coluna correta 'unit_price_cents'
		queryItem := `
			INSERT INTO order_items (order_id, name, quantity, unit_price_cents)
			VALUES ($1, $2, $3, $4)
		`

		for _, item := range req.Items {
			unitPriceCents := int(item.Price * 100)
			if _, err := tx.ExecContext(
				r.Context(),
				queryItem,
				orderID,
				item.Name,
				item.Quantity,
				unitPriceCents,
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

		// 1. Notifica o EventHub (WebSockets / Broadcast)
		eventHub.Publish(r.Context(), events.OrderEvent{
			EventID:   uuid.NewString(),
			Type:      events.OrderCreated,
			OrderID:   orderID,
			StoreID:   req.StoreID,
			WorkerID:  0,
			Items:     itemsDTO,
			TotalCent: totalCents,
			Timestamp: time.Now(),
		})

		// 2. Enfileira a tarefa no Redis se o cliente estiver configurado
		taskPayload, err := json.Marshal(OrderTask{
			OrderID:   orderID,
			StoreID:   req.StoreID,
			Items:     itemsDTO,
			TotalCent: totalCents,
		})
		if err != nil {
			log.Printf("Erro ao serializar OrderTask: %v", err)
		} else if rdb != nil {
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
}

// GetOrdersHandler retorna a lista dos últimos pedidos com seus respectivos itens
func GetOrdersHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
			return
		}

		// Consulta usando 'unit_price_cents' e convertendo para valor decimal em 'price'
		query := `
			SELECT 
				o.id, 
				o.store_id, 
				o.status, 
				o.total_cents, 
				o.created_at,
				COALESCE(
					json_agg(
						json_build_object(
							'name', oi.name,
							'quantity', oi.quantity,
							'price', (oi.unit_price_cents::numeric / 100.0)
						)
					) FILTER (WHERE oi.id IS NOT NULL), '[]'
				) AS items
			FROM orders o
			LEFT JOIN order_items oi ON o.id = oi.order_id
			GROUP BY o.id
			ORDER BY o.created_at DESC
			LIMIT 50
		`

		rows, err := db.QueryContext(r.Context(), query)
		if err != nil {
			log.Printf("Erro ao buscar pedidos: %v", err)
			http.Error(w, "Erro interno ao buscar pedidos", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		orders := make([]OrderDTO, 0)
		for rows.Next() {
			var o OrderDTO
			var itemsJSON []byte

			if err := rows.Scan(&o.ID, &o.StoreID, &o.Status, &o.TotalCents, &o.CreatedAt, &itemsJSON); err != nil {
				log.Printf("Erro ao ler linha de pedido: %v", err)
				continue
			}

			if err := json.Unmarshal(itemsJSON, &o.Items); err != nil {
				log.Printf("Erro ao deserializar itens do pedido %s: %v", o.ID, err)
				o.Items = []ItemDTO{}
			}

			orders = append(orders, o)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(orders)
	}
}

// UpdateOrderStatusHandler lida com a atualização de status do pedido pela cozinha (KDS)
func UpdateOrderStatusHandler(db *sql.DB, eventHub *events.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodPatch && r.Method != http.MethodPut {
			http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
			return
		}

		pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(pathParts) < 2 {
			http.Error(w, "ID do pedido inválido", http.StatusBadRequest)
			return
		}
		orderID := pathParts[1]

		var req UpdateStatusRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Payload inválido", http.StatusBadRequest)
			return
		}

		// Limpa o prefixo ORDER_ se presente
		statusClean := strings.TrimPrefix(req.Status, "ORDER_")
		var dbStatus string

		// Mapeia para os valores exatos do ENUM order_status do Postgres
		switch statusClean {
		case "READY", "READY_FOR_PICKUP", "READY_FOR_DELIVERY", "PRONTO":
			dbStatus = "READY_FOR_DELIVERY"
		case "IN_PREPARATION", "PREPARATION", "PREPARANDO":
			dbStatus = "IN_PREPARATION"
		case "CONFIRMED", "CONFIRMADO":
			dbStatus = "CONFIRMED"
		case "OUT_FOR_DELIVERY", "SAIU_PARA_ENTREGA":
			dbStatus = "OUT_FOR_DELIVERY"
		case "DELIVERED", "ENTREGUE":
			dbStatus = "DELIVERED"
		case "CANCELLED", "CANCELADO":
			dbStatus = "CANCELLED"
		default:
			dbStatus = statusClean
		}

		query := `UPDATE orders SET status = $1::order_status WHERE id = $2`
		res, err := db.ExecContext(r.Context(), query, dbStatus, orderID)
		if err != nil {
			log.Printf("Erro ao atualizar status no Postgres: %v", err)
			http.Error(w, "Erro ao atualizar pedido no banco", http.StatusInternalServerError)
			return
		}

		rowsAffected, _ := res.RowsAffected()
		if rowsAffected == 0 {
			http.Error(w, "Pedido não encontrado", http.StatusNotFound)
			return
		}

		// Publica evento para WebSockets
		eventTypeStr := "ORDER_" + dbStatus
		eventHub.Publish(r.Context(), events.OrderEvent{
			EventID:   uuid.NewString(),
			Type:      events.EventType(eventTypeStr),
			OrderID:   orderID,
			Status:    dbStatus,
			Timestamp: time.Now(),
		})

		log.Printf("👨‍🍳 Status do Pedido %s alterado para: %s (Evento: %s)", orderID, dbStatus, eventTypeStr)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":     "ok",
			"order_id":   orderID,
			"new_status": dbStatus,
		})
	}
}