package org.jboss.seam.infinispan.test.cacheManager.programatic;

import javax.enterprise.inject.Produces;

import org.infinispan.config.Configuration;
import org.jboss.seam.infinispan.Infinispan;

/**
 * Creates caches programatically.
 * 
 * @author pmuir
 * 
 */
public class Config {

   /**
    * Associates the "small" cache with the qualifier {@link Small}. Here we
    * test that we can still register the event bridge for the cache when it
    * isn't created by Seam.
    */
   @Produces
   @Infinispan("small")
   @Small
   Configuration configuration;

}