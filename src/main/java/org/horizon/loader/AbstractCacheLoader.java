package org.horizon.loader;

/**
 * An abstract {@link org.horizon.loader.CacheLoader} that holds common implementations for some methods
 *
 * @author Manik Surtani
 * @since 1.0
 */
public abstract class AbstractCacheLoader implements CacheLoader {

   /**
    * {@inheritDoc} This implementation delegates to {@link CacheLoader#load(Object)}, to ensure that a response is
    * returned only if the entry is not expired.
    */
   public boolean containsKey(Object key) throws CacheLoaderException {
      return load(key) != null;
   }

}
