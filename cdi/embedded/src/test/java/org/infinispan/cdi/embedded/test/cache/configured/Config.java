package org.infinispan.cdi.embedded.test.cache.configured;

import org.infinispan.cdi.embedded.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;

import jakarta.enterprise.inject.Produces;

/**
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
public class Config {
   /**
    * <p>Configures a "tiny" cache (with a very low number of entries), and associates it with the qualifier {@link
    * Tiny}.</p>
    *
    * <p>This will use the default cache container.</p>
    */
   @Tiny
   @ConfigureCache("tiny")
   @Produces
   public Configuration tinyConfiguration() {
      return new ConfigurationBuilder()
            .memory().maxCount(1)
            .build();
   }

   /**
    * <p>Configures a "small" cache (with a pretty low number of entries), and associates it with the qualifier {@link
    * Small}.</p>
    *
    * <p>This will use the default cache container.</p>
    */
   @Small
   @ConfigureCache("small")
   @Produces
   public Configuration smallConfiguration() {
      return new ConfigurationBuilder()
            .memory().maxCount(10)
            .build();
   }
}
