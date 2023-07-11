package org.infinispan.server.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.sql.DataSource;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.tasks.TaskManager;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class DummyServerManagement implements ServerManagement {

   private DefaultCacheManager defaultCacheManager;
   private Map<String, ProtocolServer> protocolServers;
   private ServerStateManager serverStateManager;

   public DummyServerManagement() {

   }

   public DummyServerManagement(EmbeddedCacheManager defaultCacheManager,
                                Map<String, ProtocolServer> protocolServers) {
      this.defaultCacheManager = (DefaultCacheManager) defaultCacheManager;
      this.protocolServers = protocolServers;
      serverStateManager = new DummyServerStateManager();

   }

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
      return defaultCacheManager;
   }

   @Override
   public ServerStateManager getServerStateManager() {
      return serverStateManager;
   }

   @Override
   public Map<String, String> getLoginConfiguration(ProtocolServer protocolServer) {
      return Collections.emptyMap();
   }

   @Override
   public Map<String, ProtocolServer> getProtocolServers() {
      return protocolServers;
   }

   @Override
   public TaskManager getTaskManager() {
      return null;
   }

   @Override
   public CompletionStage<Path> getServerReport() {
      try {
         return CompletableFuture.completedFuture(Files.createTempFile("report", ".gz"));
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
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

   @Override
   public Map<String, List<Principal>> getPrincipalList() {
      return Collections.emptyMap();
   }

   @Override
   public CompletionStage<Void> flushSecurityCaches() {
      return CompletableFutures.completedNull();
   }
}
