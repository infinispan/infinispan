package org.infinispan.server.router.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.dataconversion.GenericJbossMarshallerEncoder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.router.MultiTenantRouter;
import org.infinispan.server.router.configuration.builder.MultiTenantRouterConfigurationBuilder;
import org.infinispan.server.router.router.Router;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.hotrod.NettyHandlerRouteDestination;
import org.infinispan.server.router.routes.hotrod.SniNettyRouteSource;
import org.infinispan.server.router.utils.HotRodClientTestingUtil;
import org.infinispan.test.fwk.TestResourceTracker;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProtocolServerRouterTest {

    private final String KEYSTORE_LOCATION_FOR_HOTROD_1 = getClass().getClassLoader().getResource("sni_server_keystore.jks").getPath();
    private final String TRUSTSTORE_LOCATION_FOR_HOTROD_1 = getClass().getClassLoader().getResource("sni_client_truststore.jks").getPath();

    private final String KEYSTORE_LOCATION_FOR_HOTROD_2 = getClass().getClassLoader().getResource("default_server_keystore.jks").getPath();
    private final String TRUSTSTORE_LOCATION_FOR_HOTROD_2 = getClass().getClassLoader().getResource("default_client_truststore.jks").getPath();

    @BeforeClass
    public static void beforeClass() {
        TestResourceTracker.testStarted(ProtocolServerRouterTest.class.getName());
    }

    @AfterClass
    public static void afterClass() {
        TestResourceTracker.testFinished(ProtocolServerRouterTest.class.getName());
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
        HotRodServer hotrodServer1 = HotRodTestingUtil.startHotRodServerWithoutTransport();
        HotRodServer hotrodServer2 = HotRodTestingUtil.startHotRodServerWithoutTransport();

        NettyHandlerRouteDestination hotrod1Destination = new NettyHandlerRouteDestination("HotRod1", hotrodServer1.getInitializer());
        SniNettyRouteSource hotrod1Source = new SniNettyRouteSource("hotrod1", KEYSTORE_LOCATION_FOR_HOTROD_1, "secret".toCharArray());
        Route<SniNettyRouteSource, NettyHandlerRouteDestination> routeToHotrod1 = new Route<>(hotrod1Source, hotrod1Destination);

        NettyHandlerRouteDestination hotrod2Destination = new NettyHandlerRouteDestination("HotRod2", hotrodServer2.getInitializer());
        SniNettyRouteSource hotrod2Source = new SniNettyRouteSource("hotrod2", KEYSTORE_LOCATION_FOR_HOTROD_2, "secret".toCharArray());
        Route<SniNettyRouteSource, NettyHandlerRouteDestination> routeToHotrod2 = new Route<>(hotrod2Source, hotrod2Destination);

        MultiTenantRouterConfigurationBuilder routerConfigurationBuilder = new MultiTenantRouterConfigurationBuilder();
        routerConfigurationBuilder
                .hotrod()
                //use random port
                .port(0)
                .ip(InetAddress.getLoopbackAddress())
                .routing()
                .add(routeToHotrod1)
                .add(routeToHotrod2);

        MultiTenantRouter router = new MultiTenantRouter(routerConfigurationBuilder.build());
        router.start();

        InetAddress routerIp = router.getRouter(Router.Protocol.HOT_ROD).get().getIp().get();
        int routerPort = router.getRouter(Router.Protocol.HOT_ROD).get().getPort().get();

        //when
        RemoteCacheManager hotrod1Client = HotRodClientTestingUtil.createWithSni(routerIp, routerPort, "hotrod1", TRUSTSTORE_LOCATION_FOR_HOTROD_1, "secret".toCharArray());
        RemoteCacheManager hotrod2Client = HotRodClientTestingUtil.createWithSni(routerIp, routerPort, "hotrod2", TRUSTSTORE_LOCATION_FOR_HOTROD_2, "secret".toCharArray());

        hotrod1Client.getCache().put("test", "hotrod1");
        hotrod2Client.getCache().put("test", "hotrod2");

        //then
        // Cache storage is marshalled when written via Hot Rod client, so use the appropriate encoder
        Cache hotrod1Cache = hotrodServer1.getCacheManager().getCache().getAdvancedCache().withEncoding(GenericJbossMarshallerEncoder.class);
        Cache hotrod2Cache = hotrodServer2.getCacheManager().getCache().getAdvancedCache().withEncoding(GenericJbossMarshallerEncoder.class);
        assertThat(hotrod1Cache.size()).isEqualTo(1);
        assertThat(hotrod2Cache.size()).isEqualTo(1);
        assertThat(hotrod1Cache.get("test")).isEqualTo("hotrod1");
        assertThat(hotrod2Cache.get("test")).isEqualTo("hotrod2");
    }

}
