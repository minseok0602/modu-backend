package com.ssafy.modu.experiment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ssafy.modu.domain.edge.repository.EdgeRepository;
import com.ssafy.modu.domain.edge.service.EdgeService;
import com.ssafy.modu.domain.node.repository.NodeRepository;
import com.ssafy.modu.domain.node.service.NodeService;
import com.ssafy.modu.domain.attraction.repository.AttractionRepository;
import com.ssafy.modu.domain.routecache.repository.AttractionRouteCacheRepository;
import com.ssafy.modu.domain.routerecommend.service.*;
import com.ssafy.modu.domain.schedule.repository.ScheduleRepository;
import com.ssafy.modu.domain.schedule.service.ScheduleService;
import com.ssafy.modu.domain.user.repository.UserRepository;
import com.ssafy.modu.external.ai.AIService;
import com.ssafy.modu.external.ai.client.GmsOpenAIClient;
import com.ssafy.modu.external.ai.dto.request.RouteRecommendAiRequest;
import com.ssafy.modu.external.ai.dto.response.RouteRecommendAiResponse;
import com.ssafy.modu.external.ai.properties.GmsOpenAIProperties;
import com.ssafy.modu.external.kakao.*;
import com.ssafy.modu.external.kakao.dto.RouteSummary;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.reactive.function.client.WebClient;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = {
        "com.ssafy.modu.domain.schedule.repository", "com.ssafy.modu.domain.node.repository",
        "com.ssafy.modu.domain.edge.repository", "com.ssafy.modu.domain.routecache.repository",
        "com.ssafy.modu.domain.attraction.repository", "com.ssafy.modu.domain.user.repository"})
public class RouteExperimentConfig {
    static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
    static final ConcurrentLinkedQueue<Double> TX_MILLIS = new ConcurrentLinkedQueue<>();
    static final ConcurrentLinkedQueue<Boolean> AI_TRANSACTIONS = new ConcurrentLinkedQueue<>();
    static final ConcurrentLinkedQueue<Boolean> KAKAO_TRANSACTIONS = new ConcurrentLinkedQueue<>();
    static volatile boolean recordTransactions;

    @Bean(destroyMethod = "close")
    HikariDataSource dataSource() {
        String url = System.getProperty("ieum.jdbcUrl",
                "jdbc:mysql://127.0.0.1:13316/ieum_experiment?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Seoul");
        if (!url.startsWith("jdbc:mysql://127.0.0.1:") || !url.contains("/ieum_experiment?")) {
            throw new IllegalArgumentException("Experiment requires an isolated loopback ieum_experiment database");
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername("root");
        config.setPassword(System.getProperty("ieum.dbPassword", "ieum-local-only"));
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000);
        config.setPoolName("ieum-experiment");
        return new HikariDataSource(config);
    }

    @Bean
    LocalContainerEntityManagerFactoryBean entityManagerFactory(HikariDataSource dataSource) {
        var factory = new LocalContainerEntityManagerFactoryBean();
        factory.setDataSource(dataSource);
        factory.setPackagesToScan("com.ssafy.modu.domain");
        factory.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        factory.setJpaPropertyMap(Map.of("hibernate.hbm2ddl.auto", "create-drop",
                "hibernate.show_sql", "false", "hibernate.jdbc.time_zone", "Asia/Seoul"));
        return factory;
    }

    @Bean
    JpaTransactionManager transactionManager(EntityManagerFactory factory) {
        return new JpaTransactionManager(factory) {
            private final ThreadLocal<Long> started = new ThreadLocal<>();
            @Override protected void doBegin(Object transaction, TransactionDefinition definition) {
                super.doBegin(transaction, definition);
                if (recordTransactions) started.set(System.nanoTime());
            }
            @Override protected void doCleanupAfterCompletion(Object transaction) {
                Long begin = started.get();
                if (begin != null) {
                    TX_MILLIS.add((System.nanoTime() - begin) / 1_000_000.0);
                    started.remove();
                }
                super.doCleanupAfterCompletion(transaction);
            }
        };
    }

    @Bean(destroyMethod = "close") ProviderStub providerStub() throws IOException { return new ProviderStub(); }
    @Bean GmsOpenAIProperties openAIProperties(ProviderStub stub) {
        var properties = new GmsOpenAIProperties();
        properties.setApiKey("local-test-only");
        properties.setModel("fixed-response-stub");
        properties.setBaseUrl(stub.url() + "/chat/completions");
        properties.setRequestTimeout(Duration.ofSeconds(20));
        return properties;
    }
    @Bean KakaoMobilityProperties kakaoProperties(ProviderStub stub) {
        var properties = new KakaoMobilityProperties();
        properties.setRestApiKey("local-test-only");
        properties.setBaseUrl(stub.url());
        properties.setRequestTimeout(Duration.ofSeconds(5));
        return properties;
    }
    @Bean GmsOpenAIClient openAIClient(GmsOpenAIProperties properties) {
        return new GmsOpenAIClient(WebClient.create(), properties, JSON);
    }
    @Bean AIService aiService(GmsOpenAIClient client) {
        return new AIService(null, client, JSON) {
            @Override public RouteRecommendAiResponse recommendRoute(RouteRecommendAiRequest request) {
                AI_TRANSACTIONS.add(TransactionSynchronizationManager.isActualTransactionActive());
                return super.recommendRoute(request);
            }
        };
    }
    @Bean KakaoMobilityClient kakaoClient(KakaoMobilityProperties properties) {
        return new KakaoMobilityClient(WebClient.create(), properties) {
            @Override public RouteSummary getDirections(BigDecimal a, BigDecimal b, BigDecimal c, BigDecimal d) {
                KAKAO_TRANSACTIONS.add(TransactionSynchronizationManager.isActualTransactionActive());
                return super.getDirections(a, b, c, d);
            }
        };
    }
    @Bean EdgeService edgeService(EdgeRepository edges, NodeRepository nodes, KakaoMobilityClient kakao,
                                 AttractionRouteCacheRepository cache) { return new EdgeService(edges, nodes, kakao, cache); }
    @Bean RouteRecommendationStore store(ScheduleRepository schedules, EdgeRepository edges,
                                         AttractionRouteCacheRepository cache, EntityManager em) {
        return new RouteRecommendationStore(schedules, edges, cache, em);
    }
    @Bean RouteRecommendationResolver resolver(KakaoMobilityClient client) { return new RouteRecommendationResolver(client); }
    @Bean RouteRecommendService routeService(AIService ai, ScheduleRepository schedules, EdgeService edges,
                                             RouteRecommendationStore store, RouteRecommendationResolver resolver) {
        return new RouteRecommendService(ai, schedules, edges, store, resolver);
    }
    @Bean BaselineRouteRecommendService baseline(AIService ai, ScheduleRepository schedules, EdgeService edges) {
        return new BaselineRouteRecommendService(ai, schedules, edges);
    }
    @Bean NodeService nodeService(ScheduleRepository schedules, NodeRepository nodes,
                                  AttractionRepository attractions, EdgeService edges, ApplicationEventPublisher publisher) {
        return new NodeService(schedules, nodes, attractions, edges, publisher);
    }
    @Bean ScheduleService scheduleService(ScheduleRepository schedules, NodeRepository nodes, UserRepository users,
                                          EdgeService edges) { return new ScheduleService(schedules, nodes, users, edges); }

    static final class Gate {
        final CountDownLatch entered;
        final CountDownLatch release = new CountDownLatch(1);
        Gate(int expected) { entered = new CountDownLatch(expected); }
        void awaitEntered() throws InterruptedException {
            if (!entered.await(15, TimeUnit.SECONDS)) throw new AssertionError("Provider was not reached");
        }
    }

    /** Real HTTP transport and production JSON parsers; no external credentials. */
    static final class ProviderStub implements AutoCloseable {
        final HttpServer server;
        final ExecutorService workers = Executors.newCachedThreadPool();
        final AtomicInteger aiCalls = new AtomicInteger();
        final AtomicInteger kakaoCalls = new AtomicInteger();
        final AtomicInteger active = new AtomicInteger();
        volatile long aiDelayMs;
        volatile long kakaoDelayMs;
        volatile int aiStatus = 200;
        volatile int failKakaoAt = -1;
        volatile boolean invalidAI;
        volatile Gate aiGate;

        ProviderStub() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setExecutor(workers);
            server.createContext("/chat/completions", this::ai);
            server.createContext("/v1/directions", this::kakao);
            server.start();
        }
        String url() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        void reset() {
            aiDelayMs = 0; kakaoDelayMs = 0; aiStatus = 200; failKakaoAt = -1;
            invalidAI = false; aiGate = null; aiCalls.set(0); kakaoCalls.set(0);
            AI_TRANSACTIONS.clear(); KAKAO_TRANSACTIONS.clear();
        }
        private void ai(HttpExchange exchange) throws IOException {
            active.incrementAndGet();
            try {
                aiCalls.incrementAndGet();
                var body = JSON.readTree(exchange.getRequestBody());
                String prompt = body.path("messages").get(1).path("content").asText();
                int start = prompt.indexOf("{\"scheduleId\":");
                if (start < 0) throw new IllegalArgumentException("Missing route JSON in production prompt");
                var input = JSON.readTree(prompt.substring(start));
                ObjectNode response = JSON.createObjectNode();
                ArrayNode days = response.putArray("days");
                for (var day : input.path("days")) {
                    ObjectNode output = days.addObject();
                    output.set("date", day.path("date"));
                    output.set("nodes", day.path("nodes").deepCopy());
                    output.putArray("edges");
                    output.putObject("summary");
                }
                if (invalidAI) {
                    for (var day : days) {
                        ArrayNode nodes = (ArrayNode) day.path("nodes");
                        if (!nodes.isEmpty()) { nodes.remove(0); break; }
                    }
                }
                Gate gate = aiGate;
                if (gate != null) {
                    gate.entered.countDown();
                    if (!gate.release.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("Gate timed out");
                }
                Thread.sleep(aiDelayMs);
                if (aiStatus != 200) { send(exchange, aiStatus, "{\"error\":\"controlled failure\"}"); return; }
                send(exchange, 200, JSON.writeValueAsString(Map.of("choices", List.of(
                        Map.of("message", Map.of("content", JSON.writeValueAsString(response)))))));
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            catch (IOException ignored) { /* timeout tests intentionally close the client connection */ }
            catch (Exception e) { send(exchange, 500, JSON.writeValueAsString(Map.of("error", e.toString()))); }
            finally { active.decrementAndGet(); exchange.close(); }
        }
        private void kakao(HttpExchange exchange) throws IOException {
            active.incrementAndGet();
            try {
                int call = kakaoCalls.incrementAndGet();
                Thread.sleep(kakaoDelayMs);
                send(exchange, call == failKakaoAt ? 500 : 200,
                        "{\"routes\":[{\"result_code\":0,\"summary\":{\"distance\":1000,\"duration\":120}}]}");
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            finally { active.decrementAndGet(); exchange.close(); }
        }
        private void send(HttpExchange exchange, int status, String json) throws IOException {
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
        @Override public void close() { if (aiGate != null) aiGate.release.countDown(); server.stop(0); workers.shutdownNow(); }
    }
}
