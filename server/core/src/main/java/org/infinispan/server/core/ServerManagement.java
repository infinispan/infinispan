package org.infinispan.server.core;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.tasks.TaskManager;

/**
 * @since 10.0
 */
public interface ServerManagement {

   ConfigurationInfo getConfiguration();

   void serverStop(List<String> servers);

   void clusterStop();

   Set<String> cacheManagerNames();

   DefaultCacheManager getCacheManager(String name);

   CacheIgnoreManager getIgnoreManager(String cacheManager);

   Map<String, String> getLoginConfiguration();

   TaskManager getTaskManager();

   CompletionStage<Path> getServerReport();
}
