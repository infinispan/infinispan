package org.infinispan.server.router.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;

import org.infinispan.server.router.configuration.builder.RouterConfigurationBuilder;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.RouteDestination;
import org.infinispan.server.router.routes.RouteSource;
import org.junit.Test;

public class ConfigurationTest {

    @Test
    public void shouldBuildProperRouterConfiguration() {
        //given
        RouterConfigurationBuilder multiTenantConfigurationBuilder = new RouterConfigurationBuilder();

        RouteSource s1 = new RouteSource() {
        };
        RouteDestination d1 = new RouteDestination() {
        };

        //when
        multiTenantConfigurationBuilder
                .hotrod()
                .tcpKeepAlive(true)
                .receiveBufferSize(1)
                .sendBufferSize(1)
                .tcpNoDelay(false)
                .port(1010)
                .ip(InetAddress.getLoopbackAddress())
                .rest()
                .port(1111)
                .ip(InetAddress.getLoopbackAddress())
                .routing()
                .add(new Route(s1, d1));

        RouterConfiguration routerConfiguration = multiTenantConfigurationBuilder.build();
        HotRodRouterConfiguration hotRodRouterConfiguration = routerConfiguration.getHotRodRouterConfiguration();
        RestRouterConfiguration restRouterConfiguration = routerConfiguration.getRestRouterConfiguration();

        //then
        assertThat(hotRodRouterConfiguration.getPort()).isEqualTo(1010);
        assertThat(hotRodRouterConfiguration.getIp()).isEqualTo(InetAddress.getLoopbackAddress());
        assertThat(hotRodRouterConfiguration.tcpKeepAlive()).isTrue();
        assertThat(hotRodRouterConfiguration.tcpNoDelay()).isFalse();
        assertThat(hotRodRouterConfiguration.sendBufferSize()).isEqualTo(1);
        assertThat(hotRodRouterConfiguration.receiveBufferSize()).isEqualTo(1);

        assertThat(restRouterConfiguration.getPort()).isEqualTo(1111);
        assertThat(restRouterConfiguration.getIp()).isEqualTo(InetAddress.getLoopbackAddress());
        assertThat(routerConfiguration.getRoutingTable().routesCount()).isEqualTo(1);
    }

}
