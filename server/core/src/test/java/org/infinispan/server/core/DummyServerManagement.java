package org.infinispan.server.core;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.sql.DataSource;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.tasks.TaskManager;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class DummyServerManagement implements ServerManagement {

   @Override
   public ComponentStatus getStatus() {
      return ComponentStatus.RUNNING;
   }

   @Override
   public void serializeConfiguration(ConfigurationWriter writer) {
   }

   @Override
   public void serverStop(List<String> servers) {

   }

   @Override
   public void clusterStop() {

   }

   @Override
   public void containerStop() {
   }

   @Override
   public DefaultCacheManager getCacheManager() {
      return null;
   }

   @Override
   public ServerStateManager getServerStateManager() {
      return null;
   }

   @Override
   public Map<String, String> getLoginConfiguration(ProtocolServer protocolServer) {
      return null;
   }

   @Override
   public Map<String, ProtocolServer> getProtocolServers() {
      return null;
   }

   @Override
   public TaskManager getTaskManager() {
      return null;
   }

   @Override
   public CompletionStage<Path> getServerReport() {
      return null;
   }

   @Override
   public BackupManager getBackupManager() {
      return null;
   }

   @Override
   public Map<String, DataSource> getDataSources() {
      return null;
   }

   @Override
   public Path getServerDataPath() {
      return null;
   }
}
