package org.jboss.seam.infinispan.test.configured;

import javax.enterprise.inject.Produces;

import org.infinispan.config.Configuration;
import org.jboss.seam.infinispan.Infinispan;

public class Config {

   /**
    * Configure a "tiny" cache (with a very low number of entries), and
    * associate it with the qualifier {@link Tiny}.
    * 
    * This will use the default cache container.
    */
   @Produces
   @Infinispan("tiny")
   @Tiny
   public Configuration getTinyConfiguration() {
      Configuration configuration = new Configuration();
      configuration.setEvictionMaxEntries(1);
      return configuration;
   }

   /**
    * Configure a "small" cache (with a pretty low number of entries), and
    * associate it with the qualifier {@link Small}.
    * 
    * This will use the default cache container.
    */
   @Produces
   @Infinispan("small")
   @Small
   public Configuration getSmallConfiguration() {
      Configuration configuration = new Configuration();
      configuration.setEvictionMaxEntries(10);
      return configuration;
   }

}