package org.infinispan.server.router.routes.hotrod;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.infinispan.server.hotrod.HotRodServer;
import org.junit.jupiter.api.Test;

public class HotRodServerRouteDestinationTest {

   @Test
   public void shouldValidateName() {
      assertThrows(NullPointerException.class, () -> new HotRodServerRouteDestination(null, new HotRodServer()));
   }

   @Test
   public void shouldValidateChannelInitializer() {
      assertThrows(NullPointerException.class, () -> new HotRodServerRouteDestination("test", null));
   }

}
