package org.infinispan.cdi.test.event;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.configuration.cache.Configuration;

import javax.enterprise.inject.Produces;

/**
 * Configures two default caches - we will use both caches to check that events for one don't spill over to the other.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class Config {
   /**
    * <p>Associates the "cache1" cache with the qualifier {@link Cache1}.</p>
    *
    * <p>The default configuration will be used.</p>
    */
   @Cache1
   @ConfigureCache("cache1")
   @Produces
   public Configuration cache1Configuration;

   /**
    * <p>Associates the "cache2" cache with the qualifier {@link Cache2}.</p>
    *
    * <p>The default configuration will be used.</p>
    */
   @Cache2
   @ConfigureCache("cache2")
   @Produces
   public Configuration cache2Configuration;
}
