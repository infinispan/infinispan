package org.infinispan.hotrod;

import java.net.URI;

import org.infinispan.api.Infinispan;
import org.infinispan.api.configuration.Configuration;
import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.hotrod.impl.HotRodURI;
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
      assert configuration instanceof HotRodConfiguration;
      return new HotRod((HotRodConfiguration) configuration);
   }
}
