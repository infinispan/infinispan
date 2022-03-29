package org.infinispan.server.core;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import javax.sql.DataSource;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.tasks.TaskManager;

/**
 * @since 10.0
 */
public interface ServerManagement {

   ComponentStatus getStatus();

   void serializeConfiguration(ConfigurationWriter writer);

   void serverStop(List<String> servers);

   void clusterStop();

   void containerStop();

   /**
    * @deprecated Multiple Cache Managers are not supported in the server
    */
   @Deprecated
   default Set<String> cacheManagerNames() {
      return Collections.singleton(getCacheManager().getName());
   }

   /**
    * @deprecated Multiple Cache Managers are not supported in the server. Use {@link #getCacheManager()} instead.
    */
   @Deprecated
   default DefaultCacheManager getCacheManager(String name) {
      DefaultCacheManager cm = getCacheManager();
      return cm.getName().equals(name) ? cm : null;
   }

   DefaultCacheManager getCacheManager();

   ServerStateManager getServerStateManager();

   Map<String, String> getLoginConfiguration(ProtocolServer protocolServer);

   Map<String, ProtocolServer> getProtocolServers();

   TaskManager getTaskManager();

   CompletionStage<Path> getServerReport();

   BackupManager getBackupManager();

   Map<String, DataSource> getDataSources();

   Path getServerDataPath();
}
