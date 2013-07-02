package org.infinispan.configuration.cache;

import java.util.Properties;

/**
 *
 * AbstractLoaderConfigurationChildBuilder delegates {@link LoaderConfigurationChildBuilder} methods to a specified {@link CacheLoaderConfigurationBuilder}
 *
 * @author Pete Muir
 * @author Tristan Tarrant
 * @since 5.1
 */
public abstract class AbstractLoaderConfigurationChildBuilder<S> extends AbstractLoadersConfigurationChildBuilder implements LoaderConfigurationChildBuilder<S> {

   private final CacheLoaderConfigurationBuilder<? extends AbstractLoaderConfiguration, ? extends CacheLoaderConfigurationBuilder<?,?>> builder;

   protected AbstractLoaderConfigurationChildBuilder(CacheLoaderConfigurationBuilder<? extends AbstractLoaderConfiguration, ? extends CacheLoaderConfigurationBuilder<?,?>> builder) {
      super(builder.loaders());
      this.builder = builder;
   }

   @Override
   public S addProperty(String key, String value) {
      return (S)builder.addProperty(key, value);
   }

   @Override
   public S withProperties(Properties p) {
      return (S)builder.withProperties(p);
   }
}
