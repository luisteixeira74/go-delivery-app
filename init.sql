-- Habilita a extensão PostGIS para otimizar pesquisas de coordenadas GPS
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";

-- Enumeração para o ciclo de vida do pedido
CREATE TYPE order_status AS ENUM (
    'PENDING_PAYMENT',
    'CONFIRMED',
    'IN_PREPARATION',
    'READY_FOR_DELIVERY',
    'OUT_FOR_DELIVERY',
    'DELIVERED',
    'CANCELLED'
);

-- Tabela simplificada do Restaurante/Loja
CREATE TABLE stores (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    address TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Produtos/Cardápio
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    store_id UUID NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price_cents INT NOT NULL, -- Preço armazenado em centavos (ex: R$ 25,00 -> 2500)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Pedidos
CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    store_id UUID NOT NULL REFERENCES stores(id),
    status order_status NOT NULL DEFAULT 'PENDING_PAYMENT',
    total_cents INT NOT NULL,
    delivery_latitude DOUBLE PRECISION NOT NULL,
    delivery_longitude DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Itens do Pedido
CREATE TABLE order_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    unit_price_cents INT NOT NULL
);

-- Histórico de posições GPS registradas (Persistência fria)
-- As atualizações de posição em tempo real vão pelo Redis, mas salvamos no Postgres para histórico
CREATE TABLE delivery_locations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Dados iniciais para testes (Mocks)
INSERT INTO stores (id, name, address, latitude, longitude) VALUES 
('11111111-1111-1111-1111-111111111111', 'Lanchonete PoC Express', 'Av. Paulista, 1000 - SP', -23.561414, -46.655881);

INSERT INTO products (id, store_id, name, description, price_cents) VALUES 
('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111', 'X-Burguer Artesanal', 'Pão, carne 180g e queijo cheddar', 2890),
('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111', 'Batata Frita Média', 'Acompanha molho da casa', 1450),
('44444444-4444-4444-4444-444444444444', '11111111-1111-1111-1111-111111111111', 'Refrigerante Lata 350ml', 'Gelado', 600);