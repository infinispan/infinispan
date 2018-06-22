package org.infinispan.server.router.integration;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;

import org.assertj.core.api.Assertions;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.rest.http2.NettyHttpClient;
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
import org.infinispan.test.fwk.TestResourceTracker;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.OpenSsl;
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
        restServer = RestTestingUtil.createDefaultRestServer("default");

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
        httpClient = NettyHttpClient.newHttp2ClientWithHttp11Upgrade();
        httpClient.start("localhost", port);

        FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/default/test",
              wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));

        httpClient.sendRequest(putValueInCacheRequest);
        FullHttpResponse responses = httpClient.getResponse();

        //then
        assertThat(responses.status()).isEqualTo(HttpResponseStatus.OK);
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
        String restPrefix = String.format("/%s/%s", restServer.getConfiguration().contextPath(), cacheManager.getCacheManagerConfiguration().defaultCacheName().get());

        // First off we verify that the HTTP side of things works
        httpClient = NettyHttpClient.newHttp11Client();
        httpClient.start(host, port);

        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HTTP_1_1, POST, restPrefix + "/key", wrappedBuffer("value".getBytes(CharsetUtil.UTF_8)));
        request.trailingHeaders().add(HttpHeaderNames.CONTENT_TYPE, "text/plain");
        httpClient.sendRequest(request);
        FullHttpResponse responses = httpClient.getResponse();
        Assertions.assertThat(responses.status()).isEqualTo(HttpResponseStatus.OK);
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
        if (!OpenSsl.isAlpnSupported()) {
            throw new IllegalStateException("OpenSSL is not present, can not test TLS/ALPN support. Version: " + OpenSsl.versionString() + " Cause: " + OpenSsl.unavailabilityCause());
        }

        //given
        restServer = RestTestingUtil.createDefaultRestServer("default");

        RestServerRouteDestination restDestination = new RestServerRouteDestination("rest1", restServer);
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
        int port = router.getRouter(EndpointRouter.Protocol.SINGLE_PORT).get().getPort();

        //when
        httpClient = NettyHttpClient.newHttp2ClientWithALPN(TRUST_STORE_PATH, TRUST_STORE_PASSWORD);
        httpClient.start("localhost", port);

        FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/default/test",
              wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));

        httpClient.sendRequest(putValueInCacheRequest);
        FullHttpResponse responses = this.httpClient.getResponse();

        //then
        assertThat(responses.status()).isEqualTo(HttpResponseStatus.OK);
    }

    @Test
    public void shouldUpgradeToHotRodThroughALPN() {
        if (!OpenSsl.isAlpnSupported()) {
            throw new IllegalStateException("OpenSSL is not present, can not test TLS/ALPN support. Version: " + OpenSsl.versionString() + " Cause: " + OpenSsl.unavailabilityCause());
        }

        //given
        hotrodServer = HotRodTestingUtil.startHotRodServerWithoutTransport("default");
        restServer = RestTestingUtil.createDefaultRestServer("default");

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
}
