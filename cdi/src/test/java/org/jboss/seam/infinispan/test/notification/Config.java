package org.jboss.seam.infinispan.test.notification;

import javax.enterprise.inject.Produces;

import org.infinispan.config.Configuration;
import org.jboss.seam.infinispan.Infinispan;

/**
 * Configure two default caches - we will use both caches to check that events for one don't spill over to the other.
 */
public class Config {

   @Produces
   @Infinispan("cache1")
   @Cache1
   public Configuration getTinyConfiguration() {
      return new Configuration();
   }

   @Produces
   @Infinispan("cache2")
   @Cache2
   public Configuration getSmallConfiguration() {
      return new Configuration();
   }

}
