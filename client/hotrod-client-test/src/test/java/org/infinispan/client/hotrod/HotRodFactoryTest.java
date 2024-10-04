package org.infinispan.client.hotrod;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.net.URI;

import org.infinispan.api.Infinispan;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.junit.jupiter.api.Test;

/**
 * @since 14.0
 **/
public class HotRodFactoryTest {

   @Test
   public void testHotRodInstantiationByURI() {
      try (Infinispan infinispan = Infinispan.create(URI.create("hotrod://127.0.0.1:11222"))) {
         assertInstanceOf(HotRod.class, infinispan);
      }
   }

   @Test
   public void testHotRodInstantiationByURIasString() {
      try (Infinispan infinispan = Infinispan.create("hotrod://127.0.0.1:11222")) {
         assertInstanceOf(HotRod.class, infinispan);
      }
   }

   @Test
   public void testHotRodInstantiationByConfiguration() {
      Configuration configuration = new ConfigurationBuilder().build();
      try (Infinispan infinispan = Infinispan.create(configuration)) {
         assertInstanceOf(HotRod.class, infinispan);
      }
   }
}
