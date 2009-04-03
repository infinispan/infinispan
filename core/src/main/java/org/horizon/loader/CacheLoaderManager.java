package org.horizon.loader;

import org.horizon.lifecycle.Lifecycle;

/**
 * The cache loader manager interface
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheLoaderManager extends Lifecycle {

   CacheLoader getCacheLoader();

   CacheStore getCacheStore();

   void purge();

   boolean isUsingPassivation();

   boolean isShared();

   boolean isFetchPersistentState();

   void preload();

   boolean isEnabled();
}


