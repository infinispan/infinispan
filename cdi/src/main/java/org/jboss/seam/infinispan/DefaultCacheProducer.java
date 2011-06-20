package org.jboss.seam.infinispan;

import org.infinispan.config.Configuration;

import javax.enterprise.inject.Default;
import javax.enterprise.inject.Produces;

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
