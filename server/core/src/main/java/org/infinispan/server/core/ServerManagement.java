package org.infinispan.server.core;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import javax.sql.DataSource;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.tasks.TaskManager;

/**
 * @since 10.0
 */
public interface ServerManagement {
   void serializeConfiguration(ConfigurationWriter writer);

   void serverStop(List<String> servers);

   void clusterStop();

   Set<String> cacheManagerNames();

   DefaultCacheManager getCacheManager(String name);

   ServerStateManager getServerStateManager();

   Map<String, String> getLoginConfiguration(ProtocolServer protocolServer);

   Map<String, ProtocolServer> getProtocolServers();

   TaskManager getTaskManager();

   CompletionStage<Path> getServerReport();

   BackupManager getBackupManager();

   Map<String, DataSource> getDataSources();
}
