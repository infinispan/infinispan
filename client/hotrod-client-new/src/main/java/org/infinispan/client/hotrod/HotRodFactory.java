package org.infinispan.client.hotrod;

import java.net.URI;

import org.infinispan.api.Infinispan;
import org.infinispan.api.configuration.Configuration;
import org.infinispan.client.hotrod.impl.HotRodURI;
import org.kohsuke.MetaInfServices;

/**
 * @since 14.0
 **/
@MetaInfServices(Infinispan.Factory.class)
public class HotRodFactory implements Infinispan.Factory {
   @Override
   public Infinispan create(URI uri) {
      try {
         return new HotRod(HotRodURI.create(uri).toConfigurationBuilder().build());
      } catch (Throwable t) {
         // Not a Hot Rod URI
         return null;
      }
   }

   @Override
   public Infinispan create(Configuration configuration) {
      assert configuration instanceof org.infinispan.client.hotrod.configuration.Configuration cfg;
      return new HotRod((org.infinispan.client.hotrod.configuration.Configuration) configuration);
   }
}
