package events

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Em produção, restrinja para o domínio da sua aplicação
	},
}

type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	send      chan OrderEvent
	storeID   string // ID da loja para filtrar (se for painel do restaurante)
	courierID string // ID do entregador (se for app do entregador)
}

func (c *Client) writePump() { // 👈 Removido os parâmetros redundantes
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case event, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// 🎯 FILTRAGEM DE EVENTOS:
			// Se o cliente informou storeID, ignora eventos de outras lojas
			if c.storeID != "" && event.StoreID != c.storeID {
				continue
			}
			// Se informou courierID, ignora entregas de outros motoboys
			if c.courierID != "" && event.CourierID != c.courierID {
				continue
			}

			if err := c.conn.WriteJSON(event); err != nil {
				log.Printf("Erro ao enviar JSON via WebSocket: %v", err)
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		c.hub.Unsubscribe(c.send)
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Erro de conexão WebSocket: %v", err)
			}
			break
		}
	}
}

// ServeWs realiza o upgrade HTTP -> WebSocket e gerencia o ciclo de vida do cliente
func ServeWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Erro no upgrade de websocket: %v", err)
		return
	}

	// Lê filtros da URL: /ws?store_id=1111... ou /ws?courier_id=aaaa...
	storeID := r.URL.Query().Get("store_id")
	courierID := r.URL.Query().Get("courier_id")

	sendChan := hub.Subscribe()

	client := &Client{
		hub:       hub,
		conn:      conn,
		send:      sendChan,
		storeID:   storeID,
		courierID: courierID,
	}

	go client.writePump()
	go client.readPump()
}