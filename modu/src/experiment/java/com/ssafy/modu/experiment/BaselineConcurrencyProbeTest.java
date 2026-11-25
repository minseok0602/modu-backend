package com.ssafy.modu.experiment;

import com.ssafy.modu.domain.edge.service.EdgeService;
import com.ssafy.modu.domain.routerecommend.service.RouteRecommendationResolver;
import com.ssafy.modu.domain.routerecommend.service.RouteRecommendService;
import com.ssafy.modu.global.exception.BusinessException;
import com.ssafy.modu.global.exception.ErrorCode;
import com.ssafy.modu.external.ai.AIService;
import com.ssafy.modu.external.ai.dto.request.RouteRecommendAiRequest;
import com.ssafy.modu.external.ai.dto.response.RouteRecommendAiResponse;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

import static com.ssafy.modu.experiment.RouteExperimentConfig.*;
import static com.ssafy.modu.experiment.RouteTransactionExperimentTest.*;
import static org.assertj.core.api.Assertions.assertThat;

/** Exploratory baseline: does the original long transaction protect a manual edit? */
@Timeout(45)
class BaselineConcurrencyProbeTest {
    static final List<Map<String, Object>> observations = new ArrayList<>();
    @BeforeAll static void setup() throws Exception { start(); }
    @AfterAll static void teardown() { close(); }

    @RepeatedTest(3) void compareConcurrentManualEditWithAChangedAiRecommendation() throws Exception {
        runProbe("before");
        runProbe("after");
    }

    void runProbe(String mode) throws Exception {
        stub.reset();
        Integer constraints = new org.springframework.jdbc.core.JdbcTemplate(pool).queryForObject(
                "select count(*) from information_schema.table_constraints where table_schema = database() "
                        + "and constraint_name in ('uk_edge_schedule_from_to','uk_route_cache_from_to_provider')", Integer.class);
        assertThat(constraints).isEqualTo(2);
        Fixture fixture = seed(1, true, true).get(0);
        AIService realClient = context.getBean(AIService.class);
        AIService reversedRecommendation = new AIService(null, null, null) {
            @Override public RouteRecommendAiResponse recommendRoute(RouteRecommendAiRequest request) {
                RouteRecommendAiResponse response = realClient.recommendRoute(request);
                for (var day : response.getDays()) {
                    Collections.reverse(day.getNodes());
                    for (int i = 0; i < day.getNodes().size(); i++) day.getNodes().get(i).setVisitOrder(i + 1);
                }
                return response;
            }
        };
        // The original service body is unchanged. An explicit transaction supplies
        // exactly the transaction boundary its Spring annotation normally creates.
        var baseline = new BaselineRouteRecommendService(reversedRecommendation, schedules,
                context.getBean(EdgeService.class));
        stub.aiGate = new Gate(1);
        var improved = new RouteRecommendService(reversedRecommendation, schedules,
                context.getBean(EdgeService.class), store, context.getBean(RouteRecommendationResolver.class));
        Future<?> recommendation = workers.submit(() -> {
            if (mode.equals("before")) return tx.execute(status ->
                    baseline.autoArrange(fixture.scheduleId(), fixture.userId(), request()));
            return improved.autoArrange(fixture.scheduleId(), fixture.userId(), request());
        });
        stub.aiGate.awaitEntered();
        List<Long> manualOrder = List.of(fixture.nodeIds().get(1), fixture.nodeIds().get(0), fixture.nodeIds().get(2));
        Future<?> edit = workers.submit(() -> nodeService.updateNodeArrangement(fixture.userId(), fixture.scheduleId(),
                arrangement(fixture, DATE, List.of(1, 0, 2))));
        boolean editedBeforeAI;
        try { edit.get(750, TimeUnit.MILLISECONDS); editedBeforeAI = true; }
        catch (TimeoutException expected) { editedBeforeAI = false; }
        finally { stub.aiGate.release.countDown(); }
        boolean conflict = false;
        try { recommendation.get(10, TimeUnit.SECONDS); }
        catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOfSatisfying(BusinessException.class,
                    error -> assertThat(error.getErrorCode()).isEqualTo(ErrorCode.SCHEDULE_CHANGED_DURING_RECOMMENDATION));
            conflict = true;
        }
        edit.get(10, TimeUnit.SECONDS);
        var finalState = observe(fixture).state().nodes();
        List<Long> finalOrder = finalState.stream().sorted(Comparator.comparing(
                com.ssafy.modu.domain.routerecommend.service.RouteRecommendationStore.NodeState::order))
                .map(com.ssafy.modu.domain.routerecommend.service.RouteRecommendationStore.NodeState::id).toList();
        long uniqueOrders = finalState.stream().map(
                com.ssafy.modu.domain.routerecommend.service.RouteRecommendationStore.NodeState::order).distinct().count();
        boolean preserved = finalState.stream().allMatch(node -> Objects.equals(node.order(), manualOrder.indexOf(node.id()) + 1));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", mode);
        result.put("manual_edit_finished_before_ai_response", editedBeforeAI);
        result.put("requested_manual_order", manualOrder);
        result.put("final_order", finalOrder);
        result.put("manual_edit_preserved", preserved);
        result.put("distinct_visit_orders", uniqueOrders);
        result.put("node_count", finalState.size());
        result.put("final_nodes", finalState);
        result.put("ai_called_inside_transaction", List.copyOf(AI_TRANSACTIONS));
        result.put("recommendation_conflict", conflict);
        observations.add(result);
        Files.writeString(output.resolve("baseline-concurrent-edit.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(observations));
        System.out.println("BASELINE_CONCURRENT_EDIT " + JSON.writeValueAsString(result));
        assertThat(finalState).hasSize(3); // record the actual outcome instead of assuming a particular anomaly
        if (mode.equals("after")) {
            assertThat(editedBeforeAI).isTrue();
            assertThat(conflict).isTrue();
            assertThat(preserved).isTrue();
            assertThat(uniqueOrders).isEqualTo(3);
        }
    }
}
