package org.infinispan.server.router.integration;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.client.NettyHttpClient;
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
import org.infinispan.commons.test.TestResourceTracker;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wildfly.openssl.OpenSSLEngine;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

public class SinglePortTest {

    public static final String KEY_STORE_PATH = SinglePortTest.class.getClassLoader().getResource("./default_server_keystore.jks").getPath();
    public static final String KEY_STORE_PASSWORD = "secret";
    public static final String TRUST_STORE_PATH = SinglePortTest.class.getClassLoader().getResource("./default_client_truststore.jks").getPath();
    public static final String TRUST_STORE_PASSWORD = "secret";

    private Router router;
    private RestServer restServer;
    private HotRodServer hotrodServer;
    private NettyHttpClient httpClient;
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
    public void afterMethod() {
        RestTestingUtil.killHttpClient(httpClient);
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

    }

    @Test
    public void shouldUpgradeThroughHTTP11UpgradeHeaders() throws Exception {
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
        httpClient = NettyHttpClient.forConfiguration(builder.build());

        FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/v2/caches/default/test",
              wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));

        FullHttpResponse response = httpClient.sendRequest(putValueInCacheRequest).toCompletableFuture().get(5, TimeUnit.SECONDS);

        //then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.NO_CONTENT);
    }

    @Test
    public void shouldUpgradeToHotRodThroughHTTP11UpgradeHeaders() throws Exception {
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
        String restPrefix = String.format("/%s/v2/caches/%s", restServer.getConfiguration().contextPath(), cacheManager.getCacheManagerConfiguration().defaultCacheName().get());

        // First off we verify that the HTTP side of things works
        RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
        builder.addServer().host(host).port(port).protocol(Protocol.HTTP_11);
        httpClient = NettyHttpClient.forConfiguration(builder.build());

        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HTTP_1_1, POST, restPrefix + "/key", wrappedBuffer("value".getBytes(CharsetUtil.UTF_8)));
        request.headers().add(HttpHeaderNames.CONTENT_TYPE, "text/plain");
        FullHttpResponse response = httpClient.sendRequest(request).toCompletableFuture().get(5, TimeUnit.SECONDS);
        Assertions.assertThat(response.status()).isEqualTo(HttpResponseStatus.NO_CONTENT);
        Assertions.assertThat(restServer.getCacheManager().getCache().size()).isEqualTo(1);

        // Next up, the RemoteCacheManager
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.marshaller(new UTF8StringMarshaller());
        configurationBuilder.addServer().host(host).port(port);
        hotRodClient = new RemoteCacheManager(configurationBuilder.build());
        Object value = hotRodClient.getCache().withDataFormat(DataFormat.builder().keyType(TEXT_PLAIN).valueType(TEXT_PLAIN).build()).get("key");
        Assertions.assertThat(value).isEqualTo("value");
    }

    @Test
    public void shouldUpgradeThroughALPN() throws Exception {
        checkForOpenSSL();

        //given
        restServer = RestTestingUtil.createDefaultRestServer("rest", "default");

        RestServerRouteDestination restDestination = new RestServerRouteDestination("rest", restServer);
        SinglePortRouteSource singlePortSource = new SinglePortRouteSource();
        Route<SinglePortRouteSource, RestServerRouteDestination> routeToRest = new Route<>(singlePortSource, restDestination);

        RouterConfigurationBuilder routerConfigurationBuilder = new RouterConfigurationBuilder();
        routerConfigurationBuilder
              .singlePort()
              .sslWithAlpn(KEY_STORE_PATH, KEY_STORE_PASSWORD.toCharArray())
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
              .security().ssl().trustStoreFileName(TRUST_STORE_PATH).trustStorePassword("secret".toCharArray());
        httpClient = NettyHttpClient.forConfiguration(builder.build());

        FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/v2/caches/default/test",
              wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));

        FullHttpResponse response = httpClient.sendRequest(putValueInCacheRequest).toCompletableFuture().get(5, TimeUnit.SECONDS);

        //then
        assertThat(response.status()).isEqualTo(HttpResponseStatus.NO_CONTENT);
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

        RouterConfigurationBuilder routerConfigurationBuilder = new RouterConfigurationBuilder();
        routerConfigurationBuilder
              .singlePort()
              .sslWithAlpn(KEY_STORE_PATH, KEY_STORE_PASSWORD.toCharArray())
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
