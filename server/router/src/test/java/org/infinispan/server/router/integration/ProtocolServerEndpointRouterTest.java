package org.infinispan.server.router.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.router.Router;
import org.infinispan.server.router.configuration.builder.RouterConfigurationBuilder;
import org.infinispan.server.router.router.EndpointRouter;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.hotrod.HotRodServerRouteDestination;
import org.infinispan.server.router.routes.hotrod.SniNettyRouteSource;
import org.infinispan.server.router.utils.HotRodClientTestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProtocolServerEndpointRouterTest {

    private final String KEYSTORE_LOCATION_FOR_HOTROD_1 = getClass().getClassLoader().getResource("sni_server_keystore.jks").getPath();
    private final String TRUSTSTORE_LOCATION_FOR_HOTROD_1 = getClass().getClassLoader().getResource("sni_client_truststore.jks").getPath();

    private final String KEYSTORE_LOCATION_FOR_HOTROD_2 = getClass().getClassLoader().getResource("default_server_keystore.jks").getPath();
    private final String TRUSTSTORE_LOCATION_FOR_HOTROD_2 = getClass().getClassLoader().getResource("default_client_truststore.jks").getPath();
    private HotRodServer hotrodServer1;
    private HotRodServer hotrodServer2;
    private Router router;
    private RemoteCacheManager hotrod1Client;
    private RemoteCacheManager hotrod2Client;

    @BeforeClass
    public static void beforeClass() {
        TestResourceTracker.testStarted(ProtocolServerEndpointRouterTest.class.getName());
    }

    @AfterClass
    public static void afterClass() {
        TestResourceTracker.testFinished(ProtocolServerEndpointRouterTest.class.getName());
    }

    @After
    public void afterMethod() {
        if (router != null) {
            router.stop();
        }
        if (hotrodServer1 != null) {
            hotrodServer1.stop();
            hotrodServer1.getCacheManager().stop();
        }
        if (hotrodServer2 != null) {
            hotrodServer2.stop();
            hotrodServer2.getCacheManager().stop();
        }
        if (hotrod1Client != null) {
            hotrod1Client.stop();
        }
        if (hotrod2Client != null) {
            hotrod2Client.stop();
        }
    }

    /**
     * In this scenario we create 2 HotRod servers, each one with different credentials and SNI name. We also create a
     * new client for each server. The clients use proper TrustStores as well as SNI names.
     * <p>
     * The router should match properly SNI based routes and connect clients to proper server instances.
     */
    @Test
    public void shouldRouteToProperHotRodServerBasedOnSniHostName() throws Exception {
        //given
        hotrodServer1 = HotRodTestingUtil.startHotRodServerWithoutTransport();
        hotrodServer2 = HotRodTestingUtil.startHotRodServerWithoutTransport();

        HotRodServerRouteDestination hotrod1Destination = new HotRodServerRouteDestination("HotRod1", hotrodServer1);
        SniNettyRouteSource hotrod1Source = new SniNettyRouteSource("hotrod1", KEYSTORE_LOCATION_FOR_HOTROD_1, "secret".toCharArray());
        Route<SniNettyRouteSource, HotRodServerRouteDestination> routeToHotrod1 = new Route<>(hotrod1Source, hotrod1Destination);

        HotRodServerRouteDestination hotrod2Destination = new HotRodServerRouteDestination("HotRod2", hotrodServer2);
        SniNettyRouteSource hotrod2Source = new SniNettyRouteSource("hotrod2", KEYSTORE_LOCATION_FOR_HOTROD_2, "secret".toCharArray());
        Route<SniNettyRouteSource, HotRodServerRouteDestination> routeToHotrod2 = new Route<>(hotrod2Source, hotrod2Destination);

        RouterConfigurationBuilder routerConfigurationBuilder = new RouterConfigurationBuilder();
        routerConfigurationBuilder
                .hotrod()
                //use random port
                .port(0)
                .ip(InetAddress.getLoopbackAddress())
                .routing()
                .add(routeToHotrod1)
                .add(routeToHotrod2);

        router = new Router(routerConfigurationBuilder.build());
        router.start();

        InetAddress routerIp = router.getRouter(EndpointRouter.Protocol.HOT_ROD).get().getIp();
        int routerPort = router.getRouter(EndpointRouter.Protocol.HOT_ROD).get().getPort();

        //when
        hotrod1Client = HotRodClientTestingUtil.createWithSni(routerIp, routerPort, "hotrod1", TRUSTSTORE_LOCATION_FOR_HOTROD_1, "secret".toCharArray());
        hotrod2Client = HotRodClientTestingUtil.createWithSni(routerIp, routerPort, "hotrod2", TRUSTSTORE_LOCATION_FOR_HOTROD_2, "secret".toCharArray());

        hotrod1Client.getCache().put("test", "hotrod1");
        hotrod2Client.getCache().put("test", "hotrod2");

        //then
        Cache<String, String> hotrod1Cache = hotrodServer1.getCacheManager().getCache();
        Cache<String, String> hotrod2Cache = hotrodServer2.getCacheManager().getCache();
        assertThat(hotrod1Cache.size()).isEqualTo(1);
        assertThat(hotrod2Cache.size()).isEqualTo(1);
        assertThat(hotrod1Cache.get("test")).isEqualTo("hotrod1");
        assertThat(hotrod2Cache.get("test")).isEqualTo("hotrod2");
    }

}
