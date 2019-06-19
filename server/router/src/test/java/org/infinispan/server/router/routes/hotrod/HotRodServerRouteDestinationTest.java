package org.infinispan.server.router.routes.hotrod;

import org.infinispan.server.hotrod.HotRodServer;
import org.junit.Test;

public class HotRodServerRouteDestinationTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateName() {
        new HotRodServerRouteDestination(null, new HotRodServer()).validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateChannelInitializer() {
        new HotRodServerRouteDestination("test", null).validate();
    }

}
