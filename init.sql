-- Habilita extensões necessárias
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

-- Tabela do Restaurante / Loja
CREATE TABLE stores (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    address TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    -- Coluna nativa PostGIS (SRID 4326 = WGS 84 / GPS)
    location GEOGRAPHY(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
    ) STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Produtos / Cardápio
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    store_id UUID NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price_cents INT NOT NULL CHECK (price_cents >= 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Pedidos
CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    store_id UUID NOT NULL REFERENCES stores(id),
    status order_status NOT NULL DEFAULT 'PENDING_PAYMENT',
    total_cents INT NOT NULL CHECK (total_cents >= 0),
    delivery_latitude DOUBLE PRECISION NOT NULL,
    delivery_longitude DOUBLE PRECISION NOT NULL,
    delivery_location GEOGRAPHY(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(delivery_longitude, delivery_latitude), 4326)::geography
    ) STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Itens do Pedido
CREATE TABLE order_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    quantity INT NOT NULL DEFAULT 1 CHECK (quantity > 0),
    unit_price_cents INT NOT NULL CHECK (unit_price_cents >= 0)
);

-- Tabela de Entregadores
CREATE TABLE IF NOT EXISTS couriers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    vehicle VARCHAR(50) NOT NULL DEFAULT 'MOTORCYCLE',
    status VARCHAR(20) NOT NULL DEFAULT 'IDLE',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Histórico de posições GPS registradas (Persistência fria)
CREATE TABLE delivery_locations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    location GEOGRAPHY(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
    ) STORED,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Funcao e Trigger para atualizar updated_at automaticamente em orders
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_at_column();

-- ==============================================================================
-- ÍNDICES DE PERFORMANCE
-- ==============================================================================
CREATE INDEX idx_products_store_id ON products(store_id);
CREATE INDEX idx_orders_store_id ON orders(store_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_delivery_locations_order_id ON delivery_locations(order_id, recorded_at DESC);

-- Índices Espaciais (GIST) para o PostGIS
CREATE INDEX idx_stores_location ON stores USING GIST (location);
CREATE INDEX idx_orders_delivery_location ON orders USING GIST (delivery_location);
CREATE INDEX idx_delivery_locations_spatial ON delivery_locations USING GIST (location);

-- ==============================================================================
-- DADOS INICIAIS DE TESTE (MOCKS)
-- ==============================================================================
INSERT INTO stores (id, name, address, latitude, longitude) VALUES 
('11111111-1111-1111-1111-111111111111', 'Lanchonete PoC Express', 'Av. Paulista, 1000 - SP', -23.561414, -46.655881);

INSERT INTO products (id, store_id, name, description, price_cents) VALUES 
('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111', 'X-Burguer Artesanal', 'Pão, carne 180g e queijo cheddar', 2890),
('33333333-3333-3333-3333-333333333333', '11111111-1111-1111-1111-111111111111', 'Batata Frita Média', 'Acompanha molho da casa', 1450),
('44444444-4444-4444-4444-444444444444', '11111111-1111-1111-1111-111111111111', 'Refrigerante Lata 350ml', 'Gelado', 600);