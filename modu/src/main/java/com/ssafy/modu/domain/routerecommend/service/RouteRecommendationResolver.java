package com.ssafy.modu.domain.routerecommend.service;

import com.ssafy.modu.domain.edge.entity.Edge;
import com.ssafy.modu.domain.node.entity.Node;
import com.ssafy.modu.external.kakao.KakaoMobilityClient;
import com.ssafy.modu.external.kakao.dto.RouteSummary;
import com.ssafy.modu.global.exception.BusinessException;
import com.ssafy.modu.global.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.*;
import static com.ssafy.modu.domain.routerecommend.service.RouteRecommendationStore.*;

/** Resolve route candidates from an in-memory snapshot without a DB transaction. */
@Component
@RequiredArgsConstructor
public class RouteRecommendationResolver {
    private final KakaoMobilityClient kakaoClient;

    public ResolvedRoutes resolve(Snapshot snapshot) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("External route resolution must not run inside a database transaction");
        }
        Map<RouteKey, Metric> cache = new HashMap<>(snapshot.cache());
        Map<RouteKey, Metric> fetched = new HashMap<>();
        List<Edge> candidates = new ArrayList<>();
        List<Node> nodes = snapshot.schedule().getNodes().stream().sorted(Comparator.comparing(Node::getId)).toList();
        long candidateId = -1;
        for (Node from : nodes) {
            for (Node to : nodes) {
                if (from.getId().equals(to.getId()) || from.getVisitDate() == null
                        || !from.getVisitDate().equals(to.getVisitDate())) {
                    continue;
                }
                StoredEdge existing = snapshot.edges().get(new NodeKey(from.getId(), to.getId()));
                RouteKey routeKey = new RouteKey(from.getAttraction().getId(), to.getAttraction().getId());
                Metric metric = existing != null ? existing.metric() : cache.get(routeKey);
                if (metric == null) {
                    if (from.getAttraction().getLongitude() == null || from.getAttraction().getLatitude() == null
                            || to.getAttraction().getLongitude() == null || to.getAttraction().getLatitude() == null) {
                        throw new BusinessException(ErrorCode.INVALID_ATTRACTION_LOCATION);
                    }
                    RouteSummary response = kakaoClient.getDirections(from.getAttraction().getLongitude(),
                            from.getAttraction().getLatitude(), to.getAttraction().getLongitude(), to.getAttraction().getLatitude());
                    metric = new Metric(response.getDurationMinutes(), response.getDistanceMeters());
                    cache.put(routeKey, metric);
                    fetched.put(routeKey, metric);
                }
                candidates.add(Edge.planningCandidate(existing != null ? existing.id() : candidateId--,
                        snapshot.schedule(), from, to, metric.minutes(), metric.meters()));
            }
        }
        return new ResolvedRoutes(List.copyOf(candidates), Map.copyOf(fetched));
    }
}
