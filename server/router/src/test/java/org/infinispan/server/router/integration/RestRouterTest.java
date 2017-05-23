package org.infinispan.server.router.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;

import org.infinispan.rest.RestServer;
import org.infinispan.server.router.MultiTenantRouter;
import org.infinispan.server.router.configuration.builder.MultiTenantRouterConfigurationBuilder;
import org.infinispan.server.router.router.Router;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.rest.NettyRestServerRouteDestination;
import org.infinispan.server.router.routes.rest.RestRouteSource;
import org.infinispan.server.router.utils.RestClient;
import org.infinispan.server.router.utils.RestTestingUtil;
import org.junit.Test;

public class RestRouterTest {

    /**
     * In this scenario we create 2 REST servers, each one with different REST Path: <ul> <li>REST1 -
     * http://127.0.0.1:8080/rest/rest1</li> <li>REST2 - http://127.0.0.1:8080/rest/rest2</li> </ul>
     * <p>
     * The router should match requests based on path and redirect them to proper server.
     */
    @Test
    public void shouldRouteToProperRestServerBasedOnPath() throws Exception {
        //given
        RestServer restServer1 = RestTestingUtil.createDefaultRestServer();
        RestServer restServer2 = RestTestingUtil.createDefaultRestServer();

        NettyRestServerRouteDestination rest1Destination = new NettyRestServerRouteDestination("rest1", restServer1);
        RestRouteSource rest1Source = new RestRouteSource("rest1");
        Route<RestRouteSource, NettyRestServerRouteDestination> routeToRest1 = new Route<>(rest1Source, rest1Destination);

        NettyRestServerRouteDestination rest2Destination = new NettyRestServerRouteDestination("rest2", restServer2);
        RestRouteSource rest2Source = new RestRouteSource("rest2");
        Route<RestRouteSource, NettyRestServerRouteDestination> routeToRest2 = new Route<>(rest2Source, rest2Destination);

        MultiTenantRouterConfigurationBuilder routerConfigurationBuilder = new MultiTenantRouterConfigurationBuilder();
        routerConfigurationBuilder
                .rest()
                .port(8080)
                .ip(InetAddress.getLoopbackAddress())
                .routing()
                .add(routeToRest1)
                .add(routeToRest2);

        MultiTenantRouter router = new MultiTenantRouter(routerConfigurationBuilder.build());
        router.start();
        int port = router.getRouter(Router.Protocol.REST).get().getPort().get();

        //when
        RestClient rest1Client = new RestClient("http://127.0.0.1:" + port + "/rest/rest1");
        RestClient rest2Client = new RestClient("http://127.0.0.1:" + port + "/rest/rest2");
        rest1Client.put("test", "rest1");
        rest2Client.put("test", "rest2");
        String valueReturnedFromRest1 = rest1Client.get("test");
        String valueReturnedFromRest2 = rest2Client.get("test");

        //then
        assertThat(valueReturnedFromRest1).isEqualTo("rest1");
        assertThat(valueReturnedFromRest2).isEqualTo("rest2");
    }
}
