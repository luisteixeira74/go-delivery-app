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
	// Permite conexões do frontend durante desenvolvimento
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Client atua como ponte entre a conexão WebSocket e o Hub
type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan OrderEvent
}

// writePump consome os eventos do canal 'send' e envia via WebSocket para o navegador
func (c *Client) writePump() {
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
				// O Hub fechou o canal
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteJSON(event); err != nil {
				log.Printf("Erro ao enviar JSON para o cliente WebSocket: %v", err)
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

// readPump lida com pings/pongs e detecta o encerramento da conexão do cliente
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c.send
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		// Mantém a conexão aberta escutando mensagens (descarte no modo apenas broadcast)
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Erro na leitura da conexão WebSocket: %v", err)
			}
			break
		}
	}
}

// ServeWs é o Handler HTTP que faz o Upgrade para WebSocket
func ServeWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Erro ao realizar upgrade da conexão HTTP para WebSocket: %v", err)
		return
	}

	client := &Client{
		hub:  hub,
		conn: conn,
		send: make(chan OrderEvent, 256),
	}

	// Registra o novo cliente no Hub
	client.hub.register <- client.send

	// Inicia as goroutines de leitura e escrita para esse cliente
	go client.writePump()
	go client.readPump()
}