package com.ssafy.modu.domain.routecache.repository;

import com.ssafy.modu.domain.routecache.entity.AttractionRouteCache;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface AttractionRouteCacheRepository extends JpaRepository<AttractionRouteCache, Long> {

    // A cache miss can race across different schedules. Keep the first value
    // without turning a successful recommendation into a unique-key failure.
    @Modifying
    @Query(value = """
            insert into attraction_route_cache
              (from_attraction_id, to_attraction_id, provider, distance_meters,
               duration_minutes, calculated_at, created_at, updated_at)
            values (:fromId, :toId, :provider, :meters, :minutes,
                    current_timestamp, current_timestamp, current_timestamp)
            on duplicate key update route_cache_id = route_cache_id
            """, nativeQuery = true)
    void insertIfAbsent(Long fromId, Long toId, String provider, int meters, int minutes);

    Optional<AttractionRouteCache> findByFromAttraction_IdAndToAttraction_IdAndProvider(
            Long fromAttractionId,
            Long toAttractionId,
            String provider
    );

    List<AttractionRouteCache> findByProviderAndFromAttraction_IdInAndToAttraction_IdIn(
            String provider,
            Collection<Long> fromAttractionIds,
            Collection<Long> toAttractionIds
    );
}
