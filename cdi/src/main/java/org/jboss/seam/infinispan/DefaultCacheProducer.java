package org.jboss.seam.infinispan;

import javax.enterprise.inject.Default;
import javax.enterprise.inject.Produces;

import org.infinispan.config.Configuration;

public class DefaultCacheProducer {

   /**
    * Allows the default cache to be injected
    */
   @Produces
   @Infinispan
   @Default
   Configuration getDefaultConfiguration() {
      return new Configuration();
   }

}
