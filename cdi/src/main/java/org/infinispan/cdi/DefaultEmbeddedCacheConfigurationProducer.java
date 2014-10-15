package org.infinispan.cdi;

import org.infinispan.cdi.util.defaultbean.DefaultBean;
import org.infinispan.cdi.util.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;

import javax.enterprise.inject.Produces;

/**
 * <p>The default embedded cache {@link Configuration} producer.</p>
 *
 * <p>The default embedded cache configuration can be overridden by creating a producer which produces the new default
 * configuration. The configuration produced must have the scope {@linkplain javax.enterprise.context.Dependent Dependent}
 * and the {@linkplain javax.enterprise.inject.Default Default} qualifier.</p>
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultEmbeddedCacheConfigurationProducer {

   private static final Log log = LogFactory.getLog(DefaultEmbeddedCacheConfigurationProducer.class, Log.class);

   /**
    * Produces the default embedded cache configuration.
    *
    * @return the default embedded cache configuration.
    */
   @Produces
   @ConfigureCache
   @DefaultBean(Configuration.class)
   public Configuration getDefaultEmbeddedCacheConfiguration() {
      return new ConfigurationBuilder().build();
   }
}
