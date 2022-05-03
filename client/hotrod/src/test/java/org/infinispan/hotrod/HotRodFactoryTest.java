package org.infinispan.hotrod;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.infinispan.api.Infinispan;
import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.configuration.HotRodConfigurationBuilder;
import org.junit.jupiter.api.Test;

/**
 * @since 14.0
 **/
public class HotRodFactoryTest {

   @Test
   public void testHotRodInstantiationByURI() {
      try (Infinispan infinispan = Infinispan.create(URI.create("hotrod://127.0.0.1:11222"))) {
         assertTrue(infinispan instanceof HotRod);
      }
   }

   @Test
   public void testHotRodInstantiationByURIasString() {
      try (Infinispan infinispan = Infinispan.create("hotrod://127.0.0.1:11222")) {
         assertTrue(infinispan instanceof HotRod);
      }
   }

   @Test
   public void testHotRodInstantiationByConfiguration() {
      HotRodConfiguration configuration = new HotRodConfigurationBuilder().build();
      try (Infinispan infinispan = Infinispan.create(configuration)) {
         assertTrue(infinispan instanceof HotRod);
      }
   }
}
