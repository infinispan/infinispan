package org.infinispan.configuration.cache;

import java.util.Properties;

/**
 * LoaderConfigurationBuilder is an interface which should be implemented by all cache loader builders
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface LoaderConfigurationChildBuilder<S> extends ConfigurationChildBuilder {

   /**
    * <p>
    * Defines a single property. Can be used multiple times to define all needed properties, but the
    * full set is overridden by {@link #withProperties(Properties)}.
    * </p>
    * <p>
    * These properties are passed directly to the cache store.
    * </p>
    */
   S addProperty(String key, String value);

   /**
    * Properties passed to the cache store or loader
    * @param p
    * @return
    */
   S withProperties(Properties p);

}
