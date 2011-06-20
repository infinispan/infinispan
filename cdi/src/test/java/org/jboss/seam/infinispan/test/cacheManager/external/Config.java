package org.jboss.seam.infinispan.test.cacheManager.external;

import javax.enterprise.inject.Produces;

import org.infinispan.config.Configuration;
import org.jboss.seam.infinispan.Infinispan;

/**
 * Creates a number of caches, based on come external mechanism as de
 *
 * @author Pete Muir
 */
public class Config {

   /**
    * Associate the externally defined "large" cache with the qualifier {@link Large}
    */
   @Produces
   @Infinispan("large")
   @Large
   Configuration largeconfiguration;

   /**
    * Associate the externally defined "quick" cache with the qualifier {@link Quick}
    */
   @Produces
   @Infinispan("quick")
   @Quick
   Configuration configuration;

}
