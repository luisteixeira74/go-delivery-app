package delivery

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	CouriersGeoKey = "couriers:locations:sp"
	StoresGeoKey   = "stores:locations:sp"
)

type GeoService struct {
	rdb *redis.Client
}

func NewGeoService(rdb *redis.Client) *GeoService {
	return &GeoService{rdb: rdb}
}

// RegisterStore adiciona/atualiza uma loja no índice espacial do Redis
func (s *GeoService) RegisterStore(ctx context.Context, storeID string, lat, lng float64) error {
	return s.rdb.GeoAdd(ctx, StoresGeoKey, &redis.GeoLocation{
		Name:      storeID,
		Longitude: lng,
		Latitude:  lat,
	}).Err()
}

// UpdateCourierLocation atualiza a posição do motoboy no Redis GEO
func (s *GeoService) UpdateCourierLocation(ctx context.Context, courierID string, lat, lng float64) error {
	return s.rdb.GeoAdd(ctx, CouriersGeoKey, &redis.GeoLocation{
		Name:      courierID,
		Longitude: lng,
		Latitude:  lat,
	}).Err()
}

// RemoveCourierAvailability remove o entregador do pool de disponíveis (quando aceita entrega)
func (s *GeoService) RemoveCourierAvailability(ctx context.Context, courierID string) error {
	return s.rdb.ZRem(ctx, CouriersGeoKey, courierID).Err()
}

// FindNearestCourier busca o entregador mais próximo da loja num raio (em km)
func (s *GeoService) FindNearestCourier(ctx context.Context, storeLat, storeLng float64, radiusKM float64) (string, float64, float64, error) {
	locations, err := s.rdb.GeoSearchLocation(ctx, CouriersGeoKey, &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  storeLng,
			Latitude:   storeLat,
			Radius:     radiusKM,
			RadiusUnit: "km",
			Sort:       "ASC",
			Count:      1,
		},
		WithCoord: true,
		WithDist:  true,
	}).Result()

	if err != nil {
		return "", 0, 0, err
	}
	if len(locations) == 0 {
		return "", 0, 0, fmt.Errorf("nenhum entregador disponível no raio de %.1f km", radiusKM)
	}

	best := locations[0]
	return best.Name, best.Latitude, best.Longitude, nil
}

// CheckDeadZone verifica se o ponto de entrega do cliente fica numa zona com poucas/nenhuma loja ao redor
func (s *GeoService) CheckDeadZone(ctx context.Context, deliveryLat, deliveryLng float64, radiusKM float64) (bool, int, error) {
	locations, err := s.rdb.GeoSearch(ctx, StoresGeoKey, &redis.GeoSearchQuery{
		Longitude:  deliveryLng,
		Latitude:   deliveryLat,
		Radius:     radiusKM,
		RadiusUnit: "km",
	}).Result()

	if err != nil {
		return false, 0, err
	}

	count := len(locations)
	return count == 0, count, nil
}