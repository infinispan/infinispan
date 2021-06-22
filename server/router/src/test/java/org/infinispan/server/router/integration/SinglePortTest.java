package org.infinispan.server.router.integration;

import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.util.concurrent.CompletionStage;

import org.assertj.core.api.Assertions;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.router.Router;
import org.infinispan.server.router.configuration.builder.RouterConfigurationBuilder;
import org.infinispan.server.router.router.EndpointRouter;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.hotrod.HotRodServerRouteDestination;
import org.infinispan.server.router.routes.rest.RestServerRouteDestination;
import org.infinispan.server.router.routes.singleport.SinglePortRouteSource;
import org.infinispan.server.router.utils.RestTestingUtil;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wildfly.openssl.OpenSSLEngine;

import io.netty.util.CharsetUtil;

public class SinglePortTest {

    public static final String KEY_STORE_PATH = SinglePortTest.class.getClassLoader().getResource("./default_server_keystore.jks").getPath();
    public static final String KEY_STORE_PASSWORD = "secret";
    public static final String TRUST_STORE_PATH = SinglePortTest.class.getClassLoader().getResource("./default_client_truststore.jks").getPath();
    public static final String TRUST_STORE_PASSWORD = "secret";
    public static final RestEntity VALUE = RestEntity.create(TEXT_PLAIN, "test".getBytes(CharsetUtil.UTF_8));

    private Router router;
    private RestServer restServer;
    private HotRodServer hotrodServer;
    private RestClient httpClient;
    private RemoteCacheManager hotRodClient;

    @BeforeClass
    public static void beforeClass() {
        TestResourceTracker.testStarted(MethodHandles.lookup().lookupClass().toString());
    }

    @AfterClass
    public static void afterClass() {
        TestResourceTracker.testFinished(MethodHandles.lookup().lookupClass().toString());
    }

    @After
    public void afterMethod() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
        HotRodClientTestingUtil.killRemoteCacheManager(hotRodClient);
        RestTestingUtil.killRouter(router);
        HotRodClientTestingUtil.killServers(hotrodServer);
        if (hotrodServer != null) {
            TestingUtil.killCacheManagers(hotrodServer.getCacheManager());
        }
        if (restServer != null) {
            restServer.stop();
            TestingUtil.killCacheManagers(restServer.getCacheManager());
        }
        hotRodClient = null;
        hotrodServer = null;
        restServer = null;
    }

    @Test
    public void shouldUpgradeThroughHTTP11UpgradeHeaders() {
        //given
        restServer = RestTestingUtil.createDefaultRestServer("rest", "default");

        RestServerRouteDestination restDestination = new RestServerRouteDestination("rest1", restServer);
        SinglePortRouteSource singlePortSource = new SinglePortRouteSource();
        Route<SinglePortRouteSource, RestServerRouteDestination> routeToRest = new Route<>(singlePortSource, restDestination);

        RouterConfigurationBuilder routerConfigurationBuilder = new RouterConfigurationBuilder();
        routerConfigurationBuilder
                .singlePort()
                    .port(0)
                    .ip(InetAddress.getLoopbackAddress())
                .routing()
                    .add(routeToRest);

        router = new Router(routerConfigurationBuilder.build());
        router.start();
        int port = router.getRouter(EndpointRouter.Protocol.SINGLE_PORT).get().getPort();

        //when
        RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
        builder.addServer().host("localhost").port(port).protocol(Protocol.HTTP_20);
        httpClient = RestClient.forConfiguration(builder.build());

        CompletionStage<RestResponse> response = httpClient.cache("default").post("test", VALUE);

        //then
        ResponseAssertion.assertThat(response).hasNoContent();
    }

    @Test
    public void shouldUpgradeToHotRodThroughHTTP11UpgradeHeaders() {
        //given
        EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
        // Initialize a transport-less Hot Rod server
        HotRodServerConfigurationBuilder hotRodServerBuilder = new HotRodServerConfigurationBuilder();
        hotRodServerBuilder.startTransport(false);
        hotRodServerBuilder.name(TestResourceTracker.getCurrentTestName());
        hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager, hotRodServerBuilder);
        // Initialize a transport-less REST server
        restServer = new RestServer();
        RestServerConfigurationBuilder restServerConfigurationBuilder = new RestServerConfigurationBuilder();
        restServerConfigurationBuilder.startTransport(false);
        restServerConfigurationBuilder.name(TestResourceTracker.getCurrentTestName());
        restServer.start(restServerConfigurationBuilder.build(), cacheManager);
        // Initialize a Single Port server with routes to the Hot Rod and REST servers
        HotRodServerRouteDestination hotrodDestination = new HotRodServerRouteDestination("hotrod", hotrodServer);
        RestServerRouteDestination restDestination = new RestServerRouteDestination("rest", restServer);
        SinglePortRouteSource singlePortSource = new SinglePortRouteSource();
        Route<SinglePortRouteSource, HotRodServerRouteDestination> routeToHotRod = new Route<>(singlePortSource, hotrodDestination);
        Route<SinglePortRouteSource, RestServerRouteDestination> routeToRest = new Route<>(singlePortSource, restDestination);
        RouterConfigurationBuilder routerConfigurationBuilder = new RouterConfigurationBuilder();
        routerConfigurationBuilder
              .singlePort()
              .port(0)
              .ip(InetAddress.getLoopbackAddress())
              .routing()
              .add(routeToRest)
              .add(routeToHotRod);

        router = new Router(routerConfigurationBuilder.build());
        router.start();
        EndpointRouter endpointRouter = router.getRouter(EndpointRouter.Protocol.SINGLE_PORT).get();
        String host = endpointRouter.getHost();
        int port = endpointRouter.getPort();

        // First off we verify that the HTTP side of things works
        RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
        builder.addServer().host(host).port(port).protocol(Protocol.HTTP_11);
        httpClient = RestClient.forConfiguration(builder.build());

        CompletionStage<RestResponse> response = httpClient.cache(cacheManager.getCacheManagerConfiguration().defaultCacheName().get()).post("key", VALUE);

        ResponseAssertion.assertThat(response).hasNoContent();
        Assertions.assertThat(restServer.getCacheManager().getCache().size()).isEqualTo(1);

        // Next up, the RemoteCacheManager
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.marshaller(new UTF8StringMarshaller());
        configurationBuilder.addServer().host(host).port(port);
        hotRodClient = new RemoteCacheManager(configurationBuilder.build());
        Object value = hotRodClient.getCache().withDataFormat(DataFormat.builder().keyType(TEXT_PLAIN).valueType(TEXT_PLAIN).build()).get("key");
        Assertions.assertThat(value).isEqualTo("test");
    }

    @Test
    public void shouldUpgradeThroughALPN() {
        checkForOpenSSL();

        //given
        restServer = RestTestingUtil.createDefaultRestServer("rest", "default");

        RestServerRouteDestination restDestination = new RestServerRouteDestination("rest", restServer);
        SinglePortRouteSource singlePortSource = new SinglePortRouteSource();
        Route<SinglePortRouteSource, RestServerRouteDestination> routeToRest = new Route<>(singlePortSource, restDestination);

        SslContextFactory sslContextFactory = new SslContextFactory();
        RouterConfigurationBuilder routerConfigurationBuilder = new RouterConfigurationBuilder();
        routerConfigurationBuilder
              .singlePort()
              .sslContext(sslContextFactory.keyStoreFileName(KEY_STORE_PATH).keyStorePassword(KEY_STORE_PASSWORD.toCharArray()).getContext())
              .port(0)
              .ip(InetAddress.getLoopbackAddress())
              .routing()
              .add(routeToRest);

        router = new Router(routerConfigurationBuilder.build());
        router.start();

        EndpointRouter singlePortRouter = router.getRouter(EndpointRouter.Protocol.SINGLE_PORT).get();

        //when
        RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
        builder.addServer().host(singlePortRouter.getHost()).port(singlePortRouter.getPort()).protocol(Protocol.HTTP_20)
              .security().ssl().trustStoreFileName(TRUST_STORE_PATH).trustStorePassword("secret".toCharArray())
              .hostnameVerifier((hostname, session) -> true);
        httpClient = RestClient.forConfiguration(builder.build());

        CompletionStage<RestResponse> response = httpClient.cache("default").post("test", VALUE);

        //then
        ResponseAssertion.assertThat(response).hasNoContent();
    }

    @Test
    public void shouldUpgradeToHotRodThroughALPN() {
        checkForOpenSSL();

        //given
        hotrodServer = HotRodTestingUtil.startHotRodServerWithoutTransport("default");
        restServer = RestTestingUtil.createDefaultRestServer("rest", "default");

        HotRodServerRouteDestination hotrodDestination = new HotRodServerRouteDestination("hotrod", hotrodServer);

        RestServerRouteDestination restDestination = new RestServerRouteDestination("rest", restServer);

        SinglePortRouteSource singlePortSource = new SinglePortRouteSource();
        Route<SinglePortRouteSource, RestServerRouteDestination> routeToRest = new Route<>(singlePortSource, restDestination);
        Route<SinglePortRouteSource, HotRodServerRouteDestination> routeToHotRod = new Route<>(singlePortSource, hotrodDestination);

        SslContextFactory sslContextFactory = new SslContextFactory();
        RouterConfigurationBuilder routerConfigurationBuilder = new RouterConfigurationBuilder();
        routerConfigurationBuilder
              .singlePort()
              .sslContext(sslContextFactory.keyStoreFileName(KEY_STORE_PATH).keyStorePassword(KEY_STORE_PASSWORD.toCharArray()).getContext())
              .port(0)
              .ip(InetAddress.getLoopbackAddress())
              .routing()
              .add(routeToRest)
              .add(routeToHotRod);

        router = new Router(routerConfigurationBuilder.build());
        router.start();
        EndpointRouter endpointRouter = router.getRouter(EndpointRouter.Protocol.SINGLE_PORT).get();

        //when
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addServer().host(endpointRouter.getIp().getHostAddress()).port(endpointRouter.getPort());
        builder.security().ssl().trustStoreFileName(TRUST_STORE_PATH).trustStorePassword(TRUST_STORE_PASSWORD.toCharArray());
        hotRodClient = new RemoteCacheManager(builder.build());
        hotRodClient.getCache("default").put("test", "test");
    }

    private void checkForOpenSSL() {
        if (!OpenSSLEngine.isAlpnSupported()) {
            throw new IllegalStateException("OpenSSL is not present, can not test TLS/ALPN support.");
        }
    }
}
