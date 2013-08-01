package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.loaders.spi.CacheLoader;

/**
 * Configuration a legacy cache loader or cache store, i.e. one which doesn't provide its own builder
 *
 * @author Pete Muir
 * @since 5.1
 */
@BuiltBy(LegacyLoaderConfigurationBuilder.class)
public class LegacyLoaderConfiguration extends AbstractLoaderConfiguration {

   private final CacheLoader cacheLoader;

   LegacyLoaderConfiguration(TypedProperties properties, CacheLoader cacheLoader) {
      super(properties);
      this.cacheLoader = cacheLoader;
   }

   public CacheLoader cacheLoader() {
      return cacheLoader;
   }

   @Override
   public String toString() {
      return "LoaderConfiguration{" +
            "cacheLoader=" + cacheLoader +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      LegacyLoaderConfiguration that = (LegacyLoaderConfiguration) o;

      if (cacheLoader != null ? !cacheLoader.equals(that.cacheLoader) : that.cacheLoader != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (cacheLoader != null ? cacheLoader.hashCode() : 0);
      return result;
   }

}
