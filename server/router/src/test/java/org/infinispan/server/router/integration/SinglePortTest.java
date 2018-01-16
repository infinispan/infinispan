package org.infinispan.server.router.integration;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.util.Queue;

import org.infinispan.rest.RestServer;
import org.infinispan.rest.http2.NettyHttpClient;
import org.infinispan.rest.http2.NettyTruststoreUtil;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.server.router.Router;
import org.infinispan.server.router.configuration.builder.RouterConfigurationBuilder;
import org.infinispan.server.router.router.EndpointRouter;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.hotrod.HotRodServerRouteDestination;
import org.infinispan.server.router.routes.rest.RestServerRouteDestination;
import org.infinispan.server.router.routes.singleport.SinglePortRouteSource;
import org.infinispan.server.router.utils.RestTestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.util.CharsetUtil;

public class SinglePortTest {

    public static final String KEY_STORE_PATH = SinglePortTest.class.getClassLoader().getResource("./default_server_keystore.jks").getPath();
    public static final String KEY_STORE_PASSWORD = "secret";
    public static final String TRUST_STORE_PATH = SinglePortTest.class.getClassLoader().getResource("./default_client_truststore.jks").getPath();
    public static final String TRUST_STORE_PASSWORD = "secret";

    private Router router;

    @BeforeClass
    public static void beforeClass() {
        TestResourceTracker.testStarted(MethodHandles.lookup().lookupClass().toString());
    }

    @AfterClass
    public static void afterClass() {
        TestResourceTracker.testFinished(MethodHandles.lookup().lookupClass().toString());
    }

    @Test
    public void shouldUpgradeThroughHTTP11UpgradeHeaders() throws Exception {
        //given
        RestServer restServer1 = RestTestingUtil.createDefaultRestServer("default");

        RestServerRouteDestination restDestination = new RestServerRouteDestination("rest1", restServer1);
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
        NettyHttpClient client = NettyHttpClient.newHttp2ClientWithHttp11Upgrade();
        client.start("localhost", port);

        FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/default/test",
              wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));

        client.sendRequest(putValueInCacheRequest);
        Queue<FullHttpResponse> responses = client.getResponses();

        //then
        assertThat(responses).hasSize(1);
        assertThat(responses.poll().status()).isEqualTo(HttpResponseStatus.OK);
    }

    @Test
    public void shouldUpgradeThroughALPN() throws Exception {
        if (!OpenSsl.isAlpnSupported()) {
            throw new IllegalStateException("OpenSSL is not present, can not test TLS/ALPN support. Version: " + OpenSsl.versionString() + " Cause: " + OpenSsl.unavailabilityCause());
        }

        //given
        RestServer restServer1 = RestTestingUtil.createDefaultRestServer("default");

        RestServerRouteDestination restDestination = new RestServerRouteDestination("rest1", restServer1);
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

        Router router = new Router(routerConfigurationBuilder.build());
        router.start();
        int port = router.getRouter(EndpointRouter.Protocol.SINGLE_PORT).get().getPort();

        //when
        NettyHttpClient client = NettyHttpClient.newHttp2ClientWithALPN(TRUST_STORE_PATH, TRUST_STORE_PASSWORD);
        client.start("localhost", port);

        FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/default/test",
              wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));

        client.sendRequest(putValueInCacheRequest);
        Queue<FullHttpResponse> responses = client.getResponses();

        //then
        assertThat(responses).hasSize(1);
        assertThat(responses.poll().status()).isEqualTo(HttpResponseStatus.OK);
    }

    @Test
    public void shouldUpgradeToHotRodThroughALPN() throws Exception {
        if (!OpenSsl.isAlpnSupported()) {
            throw new IllegalStateException("OpenSSL is not present, can not test TLS/ALPN support. Version: " + OpenSsl.versionString() + " Cause: " + OpenSsl.unavailabilityCause());
        }

        //given
        HotRodServer hotrodServer = HotRodTestingUtil.startHotRodServerWithoutTransport("default");
        RestServer restServer1 = RestTestingUtil.createDefaultRestServer("default");

        HotRodServerRouteDestination hotrodDestination = new HotRodServerRouteDestination("hotrod", hotrodServer);

        RestServerRouteDestination restDestination = new RestServerRouteDestination("rest", restServer1);

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

        Router router = new Router(routerConfigurationBuilder.build());
        router.start();
        int port = router.getRouter(EndpointRouter.Protocol.SINGLE_PORT).get().getPort();

        SslContext sslContext = NettyTruststoreUtil.createTruststoreContext(TRUST_STORE_PATH, TRUST_STORE_PASSWORD.toCharArray(), "HR");

        //when
        HotRodClient hotRodClient = new HotRodClient("localhost", port, "default", 60, (byte) 20, sslContext.newEngine(ByteBufAllocator.DEFAULT));
        TestResponse response = hotRodClient.put("test", "test");

        //then
        assertThat(response.getStatus()).isEqualTo(OperationStatus.Success);
        assertThat(response.getOperation()).isEqualTo(HotRodOperation.PUT);
    }
}
