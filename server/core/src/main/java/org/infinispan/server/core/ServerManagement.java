package org.infinispan.server.core;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletionStage;

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

   CompletionStage<Void> ignoreCache(String cacheManager, String cache);

   CompletionStage<Boolean> unIgnoreCache(String cacheManager, String cache);

   CompletionStage<Collection<String>> ignoredCaches(String cacheManager);
}
