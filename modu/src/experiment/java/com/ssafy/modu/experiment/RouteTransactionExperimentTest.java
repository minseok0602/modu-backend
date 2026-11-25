package com.ssafy.modu.experiment;

import com.ssafy.modu.domain.attraction.entity.Attraction;
import com.ssafy.modu.domain.attraction.repository.AttractionRepository;
import com.ssafy.modu.domain.edge.entity.Edge;
import com.ssafy.modu.domain.edge.repository.EdgeRepository;
import com.ssafy.modu.domain.node.dto.request.NodeArrangementRequest;
import com.ssafy.modu.domain.node.dto.request.NodeCreateRequest;
import com.ssafy.modu.domain.node.entity.Node;
import com.ssafy.modu.domain.node.repository.NodeRepository;
import com.ssafy.modu.domain.node.service.NodeService;
import com.ssafy.modu.domain.routecache.entity.AttractionRouteCache;
import com.ssafy.modu.domain.routecache.repository.AttractionRouteCacheRepository;
import com.ssafy.modu.domain.routerecommend.dto.request.AutoArrangeRequest;
import com.ssafy.modu.domain.routerecommend.service.RouteRecommendationStore;
import com.ssafy.modu.domain.routerecommend.service.RouteRecommendService;
import com.ssafy.modu.domain.schedule.dto.request.ScheduleUpdateRequest;
import com.ssafy.modu.domain.schedule.dto.response.ScheduleDetailResponse;
import com.ssafy.modu.domain.schedule.entity.Schedule;
import com.ssafy.modu.domain.schedule.repository.ScheduleRepository;
import com.ssafy.modu.domain.schedule.service.ScheduleService;
import com.ssafy.modu.domain.user.entity.User;
import com.ssafy.modu.domain.user.repository.UserRepository;
import com.ssafy.modu.external.ai.properties.GmsOpenAIProperties;
import com.ssafy.modu.global.exception.BusinessException;
import com.ssafy.modu.global.exception.ErrorCode;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.nio.file.*;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static com.ssafy.modu.experiment.RouteExperimentConfig.*;
import static org.assertj.core.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(45)
class RouteTransactionExperimentTest {
    static AnnotationConfigApplicationContext context;
    static RouteRecommendService after;
    static BaselineRouteRecommendService before;
    static RouteRecommendationStore store;
    static ScheduleRepository schedules;
    static NodeRepository nodes;
    static AttractionRepository attractions;
    static EdgeRepository edges;
    static AttractionRouteCacheRepository cache;
    static UserRepository users;
    static NodeService nodeService;
    static ScheduleService scheduleService;
    static HikariDataSource pool;
    static ProviderStub stub;
    static TransactionTemplate tx;
    static ExecutorService workers;
    static Path output;
    static final LocalDate DATE = LocalDate.of(2026, 6, 1);

    record Fixture(long userId, long scheduleId, List<Long> nodeIds, List<Long> attractionIds) {}
    record Observation(RouteRecommendationStore.State state, long revision, long edgeCount, int cacheCount) {}
    record Sample(double ms, int active, int pending) {}

    @BeforeAll static void start() throws Exception {
        output = Path.of(System.getProperty("ieum.output", "build/route-experiment")).toAbsolutePath();
        Files.createDirectories(output);
        context = new AnnotationConfigApplicationContext(RouteExperimentConfig.class);
        after = context.getBean(RouteRecommendService.class);
        before = context.getBean(BaselineRouteRecommendService.class);
        store = context.getBean(RouteRecommendationStore.class);
        schedules = context.getBean(ScheduleRepository.class);
        nodes = context.getBean(NodeRepository.class);
        attractions = context.getBean(AttractionRepository.class);
        edges = context.getBean(EdgeRepository.class);
        cache = context.getBean(AttractionRouteCacheRepository.class);
        users = context.getBean(UserRepository.class);
        nodeService = context.getBean(NodeService.class);
        scheduleService = context.getBean(ScheduleService.class);
        pool = context.getBean(HikariDataSource.class);
        stub = context.getBean(ProviderStub.class);
        tx = new TransactionTemplate(context.getBean(org.springframework.transaction.PlatformTransactionManager.class));
        workers = Executors.newFixedThreadPool(10);
        Files.writeString(output.resolve("environment.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(Map.of(
                "java", System.getProperty("java.version"), "os", System.getProperty("os.name"),
                "available_processors", Runtime.getRuntime().availableProcessors(),
                "pool_size", 5, "database", "MySQL 8.0, isolated local container",
                "measurement_scope", "production service boundary and real external HTTP clients; excludes servlet/auth overhead",
                "stub", "fixed valid recommendation JSON; Kakao 1000 meters / 120 seconds",
                "sampling_ms", 10)));
    }
    @AfterAll static void close() {
        if (workers != null) workers.shutdownNow();
        if (context != null) context.close();
    }
    @BeforeEach void reset() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (stub.active.get() != 0 && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(stub.active.get()).isZero();
        stub.reset();
        context.getBean(GmsOpenAIProperties.class).setRequestTimeout(Duration.ofSeconds(20));
    }
    @AfterEach void releaseGate() { if (stub.aiGate != null) stub.aiGate.release.countDown(); }

    @Test @Order(1) void successHasNoTransactionDuringEitherProviderCall() {
        Fixture f = seed(1, false, false).get(0);
        ScheduleDetailResponse response = after.autoArrange(f.scheduleId(), f.userId(), request());
        assertThat(AI_TRANSACTIONS).containsExactly(false);
        assertThat(KAKAO_TRANSACTIONS).hasSize(6).containsOnly(false);
        assertThat(response.getDays().get(0).getNodes()).hasSize(3);
        assertThat(response.getDays().get(0).getEdges()).hasSize(2);
        assertThat(response.getDays().get(0).getEdges()).allSatisfy(edge -> assertThat(edge.getEdgeId()).isPositive());
        Observation persisted = observe(f);
        assertThat(persisted.edgeCount()).isEqualTo(6);
        assertThat(persisted.cacheCount()).isEqualTo(6);
        assertThat(persisted.revision()).isEqualTo(1);
        assertThat(persisted.state().nodes()).allSatisfy(node -> assertThat(node.date()).isEqualTo(DATE));
    }

    @Test @Order(2) void aiWaitDoesNotExposePartialPlacementOrCaches() throws Exception {
        Fixture f = seed(1, false, false).get(0);
        Observation original = observe(f);
        stub.aiGate = new Gate(1);
        Future<?> pending = workers.submit(() -> after.autoArrange(f.scheduleId(), f.userId(), request()));
        stub.aiGate.awaitEntered();
        assertThat(observe(f)).isEqualTo(original);
        assertThat(pool.getHikariPoolMXBean().getActiveConnections()).isZero();
        stub.aiGate.release.countDown();
        pending.get(10, TimeUnit.SECONDS);
    }

    @Test @Order(3) void aiHttpFailureLeavesAllDatabaseStateUnchanged() {
        Fixture f = seed(1, false, false).get(0);
        Observation original = observe(f);
        stub.aiStatus = 500;
        assertThatThrownBy(() -> after.autoArrange(f.scheduleId(), f.userId(), request())).isInstanceOf(BusinessException.class);
        assertThat(observe(f)).isEqualTo(original);
    }

    @Test @Order(4) void invalidAiResultLeavesAllDatabaseStateUnchanged() {
        Fixture f = seed(1, false, false).get(0);
        Observation original = observe(f);
        stub.invalidAI = true;
        assertThatThrownBy(() -> after.autoArrange(f.scheduleId(), f.userId(), request()))
                .isInstanceOfSatisfying(BusinessException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.INVALID_AI_RECOMMENDATION));
        assertThat(observe(f)).isEqualTo(original);
    }

    @Test @Order(5) void providerTimeoutLeavesAllDatabaseStateUnchanged() {
        Fixture f = seed(1, false, false).get(0);
        Observation original = observe(f);
        stub.aiDelayMs = 600;
        context.getBean(GmsOpenAIProperties.class).setRequestTimeout(Duration.ofMillis(100));
        assertThatThrownBy(() -> after.autoArrange(f.scheduleId(), f.userId(), request())).isInstanceOf(BusinessException.class);
        assertThat(observe(f)).isEqualTo(original);
    }

    @Test @Order(6) void partialKakaoFailureDoesNotPersistEarlierRoutes() {
        Fixture f = seed(1, false, false).get(0);
        Observation original = observe(f);
        stub.failKakaoAt = 2;
        assertThatThrownBy(() -> after.autoArrange(f.scheduleId(), f.userId(), request())).isInstanceOf(BusinessException.class);
        assertThat(stub.kakaoCalls.get()).isEqualTo(2);
        assertThat(stub.aiCalls.get()).isZero();
        assertThat(observe(f)).isEqualTo(original);
    }

    @Test @Order(7) void editsDuringAiWaitArePreservedAndOldRecommendationIsRejected() throws Exception {
        List<String> verified = new ArrayList<>();
        for (String action : List.of("reorder", "move_date", "unplace", "add", "delete", "schedule_dates", "aba_reorder")) {
            Fixture f = seed(1, true, true).get(0);
            stub.aiGate = new Gate(1);
            Future<?> pending = workers.submit(() -> after.autoArrange(f.scheduleId(), f.userId(), request()));
            stub.aiGate.awaitEntered();
            Future<?> edit = workers.submit(() -> edit(f, action));
            edit.get(3, TimeUnit.SECONDS); // must finish while the AI call is still blocked
            Observation edited = observe(f);
            stub.aiGate.release.countDown();
            assertConflict(pending);
            assertThat(observe(f)).isEqualTo(edited);
            verified.add(action);
        }
        Files.writeString(output.resolve("concurrent-edits.json"), JSON.writeValueAsString(verified));
    }

    @Test @Order(8) void scheduleDeletionDuringAiWaitRejectsOldRecommendation() throws Exception {
        Fixture f = seed(1, true, true).get(0);
        stub.aiGate = new Gate(1);
        Future<?> pending = workers.submit(() -> after.autoArrange(f.scheduleId(), f.userId(), request()));
        stub.aiGate.awaitEntered();
        workers.submit(() -> scheduleService.deleteSchedule(f.userId(), f.scheduleId())).get(3, TimeUnit.SECONDS);
        stub.aiGate.release.countDown();
        assertConflict(pending);
        assertThat(schedules.findById(f.scheduleId())).isEmpty();
    }

    @Test @Order(9) void competingRecommendationsHaveExactlyOneWinner() throws Exception {
        Fixture f = seed(1, true, true).get(0);
        stub.aiGate = new Gate(2);
        Future<?> first = workers.submit(() -> after.autoArrange(f.scheduleId(), f.userId(), request()));
        Future<?> second = workers.submit(() -> after.autoArrange(f.scheduleId(), f.userId(), request()));
        stub.aiGate.awaitEntered();
        stub.aiGate.release.countDown();
        int success = 0, conflicts = 0;
        for (Future<?> result : List.of(first, second)) {
            try { result.get(10, TimeUnit.SECONDS); success++; }
            catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOfSatisfying(BusinessException.class,
                        error -> assertThat(error.getErrorCode()).isEqualTo(ErrorCode.SCHEDULE_CHANGED_DURING_RECOMMENDATION));
                conflicts++;
            }
        }
        assertThat(success).isEqualTo(1);
        assertThat(conflicts).isEqualTo(1);
        assertThat(observe(f).revision()).isEqualTo(1);
    }

    @Test @Order(10) void concurrentCacheMissAcrossSchedulesDoesNotCauseUniqueKeyFailure() throws Exception {
        List<Fixture> fixtures = seed(2, false, false);
        stub.aiGate = new Gate(2);
        var pending = fixtures.stream().map(f -> workers.submit(
                () -> after.autoArrange(f.scheduleId(), f.userId(), request()))).toList();
        stub.aiGate.awaitEntered();
        stub.aiGate.release.countDown();
        for (Future<?> result : pending) result.get(10, TimeUnit.SECONDS);
        for (Fixture f : fixtures) {
            assertThat(observe(f).cacheCount()).isEqualTo(6);
            assertThat(observe(f).edgeCount()).isEqualTo(6);
        }
    }

    @Test @Order(11) void finalDatabaseFailureRollsBackNodesEdgesRevisionAndNewCaches() {
        Fixture f = seed(1, false, false).get(0);
        Observation original = observe(f);
        JdbcTemplate jdbc = new JdbcTemplate(pool);
        jdbc.execute("CREATE TRIGGER ieum_reject_edge BEFORE INSERT ON edge FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'controlled apply failure'");
        try {
            assertThatThrownBy(() -> after.autoArrange(f.scheduleId(), f.userId(), request())).isInstanceOf(RuntimeException.class);
            assertThat(observe(f)).isEqualTo(original);
        } finally { jdbc.execute("DROP TRIGGER ieum_reject_edge"); }
    }

    @Test @Order(12) void enclosingWorkflowKeepsItsOriginalRollbackContract() {
        Fixture f = seed(1, false, false).get(0);
        Observation original = observe(f);
        assertThatThrownBy(() -> tx.execute(status -> {
            after.autoArrange(f.scheduleId(), f.userId(), request());
            throw new IllegalStateException("controlled outer rollback");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(AI_TRANSACTIONS).containsExactly(true);
        assertThat(observe(f)).isEqualTo(original);
    }

    @Test @Order(13) void wrongOwnerDoesNotReachExternalProviders() {
        Fixture f = seed(1, false, false).get(0);
        assertThatThrownBy(() -> after.autoArrange(f.scheduleId(), -1L, request())).isInstanceOfSatisfying(
                BusinessException.class, e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.SCHEDULE_NOT_FOUND));
        assertThat(stub.aiCalls.get()).isZero();
        assertThat(stub.kakaoCalls.get()).isZero();
    }

    @Test @Order(14) void explicitStartAndEndArePreserved() {
        Fixture f = seed(1, false, false).get(0);
        AutoArrangeRequest request = request();
        var day = new AutoArrangeRequest.DayCondition();
        day.setDate(DATE); day.setStartNodeId(f.nodeIds().get(0)); day.setEndNodeId(f.nodeIds().get(2));
        request.setDays(List.of(day));
        var response = after.autoArrange(f.scheduleId(), f.userId(), request);
        assertThat(response.getDays().get(0).getNodes().get(0).getNodeId()).isEqualTo(f.nodeIds().get(0));
        assertThat(response.getDays().get(0).getNodes().get(2).getNodeId()).isEqualTo(f.nodeIds().get(2));
    }

    @Test @Order(100) @Timeout(360) void resourceExperiment() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("ieum.performance"), "Enable with -Dieum.performance=true");
        List<String> summary = new ArrayList<>(List.of("mode,scenario,delay_ms,kakao_delay_ms,concurrency,repetition,successes,failures,p50_ms,p95_ms,wall_ms,active_avg,active_max,pending_max,connection_ms,tx_count,tx_mean_ms,tx_max_ms,ai_calls,ai_in_transaction,kakao_calls"));
        List<String> timeseries = new ArrayList<>(List.of("run,elapsed_ms,active,pending"));
        // Warm production HTTP clients, Hibernate, and both service paths separately.
        for (String mode : List.of("before", "after")) {
            for (Fixture f : seed(5, true, true)) invoke(mode, f);
        }
        for (int delay : List.of(0, 2000, 5000)) {
            for (int concurrency : List.of(1, 5, 10)) {
                for (int repetition = 1; repetition <= 3; repetition++) {
                    // Alternate order across repeats to reduce a systematic order advantage.
                    for (String mode : repetition % 2 == 0 ? List.of("after", "before") : List.of("before", "after")) {
                        runMeasurement(mode, delay, concurrency, repetition, false, summary, timeseries);
                        Files.write(output.resolve("summary.csv"), summary);
                        Files.write(output.resolve("timeseries.csv"), timeseries);
                    }
                }
            }
        }
        for (int repetition = 1; repetition <= 3; repetition++) {
            for (String mode : repetition % 2 == 0 ? List.of("after", "before") : List.of("before", "after")) {
                runMeasurement(mode, 2000, 5, repetition, true, summary, timeseries);
                Files.write(output.resolve("summary.csv"), summary);
                Files.write(output.resolve("timeseries.csv"), timeseries);
            }
        }
        assertThat(summary).hasSize(61);
    }

    static void runMeasurement(String mode, int delay, int concurrency, int repetition, boolean cold,
                               List<String> summary, List<String> timeseries) throws Exception {
        List<Fixture> fixtures = cold ? java.util.stream.IntStream.range(0, concurrency)
                .mapToObj(i -> seed(1, false, false).get(0)).toList() : seed(concurrency, true, true);
        stub.reset(); stub.aiDelayMs = delay; stub.kakaoDelayMs = cold ? 100 : 0;
        TX_MILLIS.clear(); recordTransactions = true;
        var ready = new CountDownLatch(concurrency);
        var go = new CountDownLatch(1);
        List<Double> durations = Collections.synchronizedList(new ArrayList<>());
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        List<Future<?>> pending = new ArrayList<>();
        for (Fixture f : fixtures) pending.add(workers.submit(() -> {
            ready.countDown();
            try {
                go.await(); long begin = System.nanoTime();
                invoke(mode, f);
                durations.add((System.nanoTime() - begin) / 1_000_000.0);
            } catch (Throwable error) { failures.add(error); }
        }));
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        List<Sample> samples = Collections.synchronizedList(new ArrayList<>());
        var sampler = Executors.newSingleThreadScheduledExecutor();
        long begin = System.nanoTime();
        sampler.scheduleAtFixedRate(() -> samples.add(new Sample((System.nanoTime() - begin) / 1_000_000.0,
                pool.getHikariPoolMXBean().getActiveConnections(), pool.getHikariPoolMXBean().getThreadsAwaitingConnection())), 0, 10, TimeUnit.MILLISECONDS);
        try {
            go.countDown();
            for (Future<?> result : pending) result.get(35, TimeUnit.SECONDS);
        } finally { recordTransactions = false; sampler.shutdownNow(); sampler.awaitTermination(2, TimeUnit.SECONDS); }
        double wall = (System.nanoTime() - begin) / 1_000_000.0;
        double area = 0;
        for (int i = 0; i < samples.size(); i++) {
            Sample current = samples.get(i);
            double next = i + 1 < samples.size() ? samples.get(i + 1).ms() : wall;
            area += current.active() * Math.max(0, next - current.ms());
        }
        String scenario = cold ? "cold_routes" : "ai_only";
        String run = mode + "-" + scenario + "-" + delay + "-" + concurrency + "-" + repetition;
        for (Sample sample : samples) timeseries.add(String.format(Locale.ROOT, "%s,%.3f,%d,%d", run, sample.ms(), sample.active(), sample.pending()));
        String row = String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.4f,%d,%d,%.3f,%d,%.3f,%.3f,%d,%d,%d",
                mode, scenario, delay, cold ? 100 : 0, concurrency, repetition, durations.size(), failures.size(), percentile(durations, .5), percentile(durations, .95), wall,
                area / wall, samples.stream().mapToInt(Sample::active).max().orElse(0), samples.stream().mapToInt(Sample::pending).max().orElse(0), area,
                TX_MILLIS.size(), TX_MILLIS.stream().mapToDouble(Double::doubleValue).average().orElse(0), TX_MILLIS.stream().mapToDouble(Double::doubleValue).max().orElse(0),
                stub.aiCalls.get(), AI_TRANSACTIONS.stream().filter(Boolean::booleanValue).count(), stub.kakaoCalls.get());
        summary.add(row); System.out.println("MEASUREMENT " + row);
        assertThat(failures).isEmpty();
        if (cold) assertThat(stub.kakaoCalls.get()).isEqualTo(6 * concurrency);
        else assertThat(KAKAO_TRANSACTIONS).isEmpty(); // cached/preplaced main experiment isolates AI delay
        assertThat(AI_TRANSACTIONS).hasSize(concurrency).containsOnly(mode.equals("before"));
        assertThat(durations).hasSize(concurrency);
        for (Fixture f : fixtures) assertThat(observe(f).state().nodes()).hasSize(3);
    }

    static double percentile(List<Double> values, double fraction) {
        if (values.isEmpty()) return Double.NaN;
        List<Double> sorted = values.stream().sorted().toList();
        return sorted.get(Math.max(0, (int) Math.ceil(fraction * sorted.size()) - 1));
    }
    static void invoke(String mode, Fixture f) {
        if (mode.equals("before")) before.autoArrange(f.scheduleId(), f.userId(), request());
        else after.autoArrange(f.scheduleId(), f.userId(), request());
    }
    static AutoArrangeRequest request() { return new AutoArrangeRequest(); }
    static Observation observe(Fixture f) {
        var snapshot = store.load(f.scheduleId(), f.userId());
        return new Observation(snapshot.original(), snapshot.revision(), snapshot.edges().size(), snapshot.cache().size());
    }
    static void assertConflict(Future<?> pending) throws Exception {
        try { pending.get(10, TimeUnit.SECONDS); throw new AssertionError("Expected stale recommendation conflict"); }
        catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOfSatisfying(BusinessException.class,
                    error -> assertThat(error.getErrorCode()).isEqualTo(ErrorCode.SCHEDULE_CHANGED_DURING_RECOMMENDATION));
        }
    }
    static void edit(Fixture f, String action) {
        switch (action) {
            case "reorder", "aba_reorder" -> {
                nodeService.updateNodeArrangement(f.userId(), f.scheduleId(), arrangement(f, DATE, List.of(2, 1, 0)));
                if (action.equals("aba_reorder")) nodeService.updateNodeArrangement(f.userId(), f.scheduleId(), arrangement(f, DATE, List.of(0, 1, 2)));
            }
            case "move_date" -> {
                tx.executeWithoutResult(status -> {
                    Schedule schedule = schedules.findLockedByIdAndUserId(f.scheduleId(), f.userId()).orElseThrow();
                    schedule.update(schedule.getTitle(), DATE, DATE.plusDays(1));
                });
                nodeService.updateNodeArrangement(f.userId(), f.scheduleId(), arrangement(f, DATE.plusDays(1), List.of(0, 1, 2)));
            }
            case "unplace" -> nodeService.updateNodeArrangement(f.userId(), f.scheduleId(), arrangement(f, null, List.of(0, 1, 2)));
            case "add" -> {
                NodeCreateRequest request = new NodeCreateRequest(); request.setAttractionId(f.attractionIds().get(0));
                nodeService.addNode(f.userId(), f.scheduleId(), request);
            }
            case "delete" -> nodeService.deleteNode(f.userId(), f.scheduleId(), f.nodeIds().get(1));
            case "schedule_dates" -> {
                ScheduleUpdateRequest request = new ScheduleUpdateRequest();
                ReflectionTestUtils.setField(request, "title", "edited while AI waited");
                ReflectionTestUtils.setField(request, "startDate", DATE);
                ReflectionTestUtils.setField(request, "endDate", DATE.plusDays(1));
                scheduleService.updateSchedule(f.userId(), f.scheduleId(), request);
            }
            default -> throw new IllegalArgumentException(action);
        }
    }
    static NodeArrangementRequest arrangement(Fixture f, LocalDate date, List<Integer> order) {
        List<NodeArrangementRequest.NodeArrangement> placements = new ArrayList<>();
        for (int i = 0; i < order.size(); i++) {
            placements.add(NodeArrangementRequest.NodeArrangement.of(f.nodeIds().get(order.get(i)), date == null ? null : i + 1));
        }
        return NodeArrangementRequest.of(List.of(NodeArrangementRequest.DayArrangement.of(date, placements)));
    }
    static List<Fixture> seed(int count, boolean placed, boolean warm) {
        return tx.execute(status -> {
            List<Attraction> places = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                Attraction attraction = Attraction.create("exp-" + UUID.randomUUID().toString().substring(0, 16), List.of("12", "14", "39").get(i));
                ReflectionTestUtils.setField(attraction, "name", "test attraction " + i);
                ReflectionTestUtils.setField(attraction, "latitude", BigDecimal.valueOf(35.1 + .01 * i));
                ReflectionTestUtils.setField(attraction, "longitude", BigDecimal.valueOf(128.1 + .01 * i));
                places.add(attractions.save(attraction));
            }
            if (warm) for (Attraction from : places) for (Attraction to : places) if (from != to) {
                cache.save(AttractionRouteCache.create(from, to, AttractionRouteCache.PROVIDER_KAKAO, 1000, 2));
            }
            List<Fixture> fixtures = new ArrayList<>();
            for (int j = 0; j < count; j++) {
                String unique = UUID.randomUUID().toString().substring(0, 12);
                User user = users.save(User.createSocialUser("exp-" + unique, "unused-test-password", unique, null));
                Schedule schedule = schedules.save(Schedule.create(user, "experiment", DATE, DATE));
                List<Node> created = new ArrayList<>();
                for (int i = 0; i < places.size(); i++) {
                    Node node = Node.from(places.get(i)); schedule.addNode(node);
                    if (placed) node.updateVisitInfo(i + 1, DATE);
                    created.add(nodes.save(node));
                }
                if (warm) for (Node from : created) for (Node to : created) if (from != to) edges.save(Edge.create(schedule, from, to, 2, 1000));
                fixtures.add(new Fixture(user.getId(), schedule.getId(), created.stream().map(Node::getId).toList(), places.stream().map(Attraction::getId).toList()));
            }
            return fixtures;
        });
    }
}
