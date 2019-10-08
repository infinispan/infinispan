package org.infinispan.server.core;

import java.util.Set;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.manager.DefaultCacheManager;

/**
 * @since 10.0
 */
public interface ServerManagement {

   ConfigurationInfo getConfiguration();

   void stop();

   Set<String> cacheManagerNames();

   DefaultCacheManager getCacheManager(String name);

   CacheIgnoreManager getIgnoreManager(String cacheManager);

}
