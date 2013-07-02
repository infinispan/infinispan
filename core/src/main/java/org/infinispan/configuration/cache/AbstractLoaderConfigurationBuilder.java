package org.infinispan.configuration.cache;

import java.util.Properties;

/*
 * This is slightly different AbstractLoaderConfigurationChildBuilder, as it instantiates a new set of children (async and singletonStore)
 * rather than delegate to existing ones.
 */
public abstract class AbstractLoaderConfigurationBuilder<T extends CacheLoaderConfiguration, S extends AbstractLoaderConfigurationBuilder<T, S>> extends
      AbstractLoadersConfigurationChildBuilder implements CacheLoaderConfigurationBuilder<T, S> {
   protected Properties properties = new Properties();

   public AbstractLoaderConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * <p>
    * Defines a single property. Can be used multiple times to define all needed properties, but the
    * full set is overridden by {@link #withProperties(Properties)}.
    * </p>
    * <p>
    * These properties are passed directly to the cache store.
    * </p>
    */
   @Override
   public S addProperty(String key, String value) {
      this.properties.put(key, value);
      return self();
   }

   /**
    * <p>
    * These properties are passed directly to the cache store.
    * </p>
    */
   @Override
   public S withProperties(Properties props) {
      this.properties = props;
      return self();
   }
}
