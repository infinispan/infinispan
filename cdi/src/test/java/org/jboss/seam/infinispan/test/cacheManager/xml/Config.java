package org.jboss.seam.infinispan.test.cacheManager.xml;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.infinispan.config.Configuration;
import org.jboss.seam.infinispan.Infinispan;

@ApplicationScoped
public class Config {

   /**
    * Associate the "very-large" cache (configured below) with the qualifier
    * {@link VeryLarge}.
    */
   @Produces
   @Infinispan("very-large")
   @VeryLarge
   Configuration veryLargeCacheContainer;

   /**
    * Associate the "quick-very-large" cache (configured below) with the
    * qualifier {@link Quick}.
    */
   @Produces
   @Infinispan("quick-very-large")
   @Quick
   Configuration quickVeryLargeCacheContainer;

}