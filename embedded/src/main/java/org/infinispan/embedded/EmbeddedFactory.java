package org.infinispan.embedded;

import java.net.URI;

import org.infinispan.api.Infinispan;
import org.infinispan.api.configuration.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.kohsuke.MetaInfServices;

/**
 * @since 15.0
 */
@MetaInfServices(Infinispan.Factory.class)
public class EmbeddedFactory implements Infinispan.Factory {
   @Override
   public Infinispan create(URI uri) {
      try {
         return new Embedded(EmbeddedURI.create(uri).toConfiguration());
      } catch (Throwable t) {
         // Not a Hot Rod URI
         return null;
      }
   }

   @Override
   public Infinispan create(Configuration configuration) {
      assert configuration instanceof GlobalConfiguration;
      return new Embedded((GlobalConfiguration) configuration);
   }
}
