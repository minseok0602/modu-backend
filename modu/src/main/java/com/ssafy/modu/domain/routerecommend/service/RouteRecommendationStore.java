package com.ssafy.modu.domain.routerecommend.service;

import com.ssafy.modu.domain.edge.entity.Edge;
import com.ssafy.modu.domain.edge.repository.EdgeRepository;
import com.ssafy.modu.domain.node.entity.Node;
import com.ssafy.modu.domain.routecache.entity.AttractionRouteCache;
import com.ssafy.modu.domain.routecache.repository.AttractionRouteCacheRepository;
import com.ssafy.modu.domain.schedule.dto.response.ScheduleDetailResponse;
import com.ssafy.modu.domain.schedule.entity.Schedule;
import com.ssafy.modu.domain.schedule.repository.ScheduleRepository;
import com.ssafy.modu.global.exception.BusinessException;
import com.ssafy.modu.global.exception.ErrorCode;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Short database phases only. No external calls are allowed in this component. */
@Service
@RequiredArgsConstructor
public class RouteRecommendationStore {
    private final ScheduleRepository scheduleRepository;
    private final EdgeRepository edgeRepository;
    private final AttractionRouteCacheRepository cacheRepository;
    private final EntityManager entityManager;

    public record NodeKey(long fromId, long toId) {}
    public record RouteKey(long fromId, long toId) {}
    public record Metric(int minutes, int meters) {}
    public record StoredEdge(long id, Metric metric) {}
    public record NodeState(long id, long attractionId, LocalDate date, Integer order,
                            BigDecimal latitude, BigDecimal longitude, String contentType) {}
    public record State(String title, LocalDate start, LocalDate end, boolean shared,
                        List<NodeState> nodes) {
        static State of(Schedule schedule) {
            return new State(schedule.getTitle(), schedule.getStartDate(), schedule.getEndDate(),
                    schedule.isArrivalShared(), schedule.getNodes().stream()
                    .sorted(Comparator.comparing(Node::getId))
                    .map(node -> new NodeState(node.getId(), node.getAttraction().getId(),
                            node.getVisitDate(), node.getVisitOrder(),
                            node.getAttraction().getLatitude(), node.getAttraction().getLongitude(),
                            node.getAttraction().getContentTypeId())).toList());
        }
    }
    // The initialized entity graph is detached before returning and is exclusively
    // owned by this request. Planning mutations never dirty managed database rows.
    public record Snapshot(Schedule schedule, long revision, State original,
                           Map<NodeKey, StoredEdge> edges, Map<RouteKey, Metric> cache) {}
    public record ResolvedRoutes(List<Edge> candidates, Map<RouteKey, Metric> fetched) {}

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Snapshot load(Long scheduleId, Long userId) {
        scheduleRepository.findLockedByIdAndUserId(scheduleId, userId)
                .orElseThrow(() -> new BusinessException(ErrorCode.SCHEDULE_NOT_FOUND));
        Schedule schedule = scheduleRepository.findWithNodesByIdAndUser_Id(scheduleId, userId)
                .orElseThrow(() -> new BusinessException(ErrorCode.SCHEDULE_NOT_FOUND));
        State original = State.of(schedule);

        Map<NodeKey, StoredEdge> edges = new HashMap<>();
        for (Edge edge : edgeRepository.findByScheduleId(scheduleId)) {
            edges.put(new NodeKey(edge.getFromNode().getId(), edge.getToNode().getId()),
                    new StoredEdge(edge.getId(), new Metric(edge.getEstimatedTimeMinutes(), edge.getDistanceMeters())));
        }
        Set<Long> attractionIds = schedule.getNodes().stream()
                .map(node -> node.getAttraction().getId()).collect(Collectors.toSet());
        Map<RouteKey, Metric> cache = new HashMap<>();
        if (!attractionIds.isEmpty()) {
            for (AttractionRouteCache entry : cacheRepository.findByProviderAndFromAttraction_IdInAndToAttraction_IdIn(
                    AttractionRouteCache.PROVIDER_KAKAO, attractionIds, attractionIds)) {
                cache.put(new RouteKey(entry.getFromAttraction().getId(), entry.getToAttraction().getId()),
                        new Metric(entry.getDurationMinutes(), entry.getDistanceMeters()));
            }
        }
        long revision = schedule.getRouteRevision();
        for (Node node : schedule.getNodes()) {
            // All scalar attraction fields used by planning have been initialized.
            entityManager.detach(node.getAttraction());
        }
        entityManager.detach(schedule); // cascade ALL also detaches the initialized nodes
        return new Snapshot(schedule, revision, original, Map.copyOf(edges), Map.copyOf(cache));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ScheduleDetailResponse apply(Long userId, Snapshot snapshot, ResolvedRoutes routes) {
        Long scheduleId = snapshot.schedule().getId();
        Schedule schedule = scheduleRepository.findLockedByIdAndUserId(scheduleId, userId)
                .orElseThrow(RouteRecommendationStore::conflict);
        schedule = scheduleRepository.findWithNodesByIdAndUser_Id(scheduleId, userId)
                .orElseThrow(RouteRecommendationStore::conflict);
        // The lock remains held through validation, all writes, and commit.
        // A concurrent child edit must obtain this same parent lock first.
        if (schedule.getRouteRevision() != snapshot.revision()
                || !State.of(schedule).equals(snapshot.original())) {
            throw conflict();
        }

        Map<Long, Node> currentNodes = schedule.getNodes().stream()
                .collect(Collectors.toMap(Node::getId, Function.identity()));
        for (Node planned : snapshot.schedule().getNodes()) {
            currentNodes.get(planned.getId()).updateVisitInfo(planned.getVisitOrder(), planned.getVisitDate());
        }
        schedule.advanceRouteRevision();

        // Stable key ordering limits cross-schedule cache deadlocks. The unique-key
        // upsert safely handles two schedules fetching the same previously missing route.
        routes.fetched().entrySet().stream()
                .sorted(Comparator.comparingLong((Map.Entry<RouteKey, Metric> e) -> e.getKey().fromId())
                        .thenComparingLong(e -> e.getKey().toId()))
                .forEach(entry -> cacheRepository.insertIfAbsent(entry.getKey().fromId(), entry.getKey().toId(),
                        AttractionRouteCache.PROVIDER_KAKAO, entry.getValue().meters(), entry.getValue().minutes()));

        Set<NodeKey> existing = edgeRepository.findByScheduleId(scheduleId).stream()
                .map(edge -> new NodeKey(edge.getFromNode().getId(), edge.getToNode().getId()))
                .collect(Collectors.toSet());
        List<Edge> missing = new ArrayList<>();
        for (Edge candidate : routes.candidates()) {
            NodeKey key = new NodeKey(candidate.getFromNode().getId(), candidate.getToNode().getId());
            if (existing.add(key)) {
                missing.add(Edge.create(schedule, currentNodes.get(key.fromId()), currentNodes.get(key.toId()),
                        candidate.getEstimatedTimeMinutes(), candidate.getDistanceMeters()));
            }
        }
        edgeRepository.saveAll(missing);
        // Conversion stays in the transaction; the HTTP response exposes real DB IDs.
        return ScheduleDetailResponse.from(schedule, edgeRepository.findByScheduleId(scheduleId));
    }

    private static BusinessException conflict() {
        return new BusinessException(ErrorCode.SCHEDULE_CHANGED_DURING_RECOMMENDATION);
    }
}
