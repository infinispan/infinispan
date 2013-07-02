package org.infinispan.configuration.cache;

import org.infinispan.loaders.CacheLoaderConfig;

/**
 * LegacyLoaderAdapter. This interface should disappear in 6.0
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Deprecated
public interface LegacyLoaderAdapter<T extends CacheLoaderConfig> {
   T adapt();
}
