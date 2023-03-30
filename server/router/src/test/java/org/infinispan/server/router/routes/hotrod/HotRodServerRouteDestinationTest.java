package org.infinispan.server.router.routes.hotrod;

import org.infinispan.server.hotrod.HotRodServer;
import org.junit.Test;

public class HotRodServerRouteDestinationTest {

    @Test(expected = NullPointerException.class)
    public void shouldValidateName() {
        new HotRodServerRouteDestination(null, new HotRodServer());
    }

    @Test(expected = NullPointerException.class)
    public void shouldValidateChannelInitializer() {
        new HotRodServerRouteDestination("test", null);
    }

}
