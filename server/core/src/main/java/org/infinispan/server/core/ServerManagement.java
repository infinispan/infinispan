package org.infinispan.server.core;

import java.nio.file.Path;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.sql.DataSource;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.tasks.manager.TaskManager;

/**
 * Provides management operations and metadata for an Infinispan server instance.
 *
 * @since 10.0
 */
public interface ServerManagement {

   /**
    * These constants are used as keys in the Map returned by {@link #getLoginConfiguration(ProtocolServer)}.
    */
   String MODE = "mode";
   String URL = "url";
   String REALM = "realm";
   String CLIENT_ID = "clientId";

   /**
    * Returns the current status of the server.
    */
   ComponentStatus getStatus();

   /**
    * Serializes the server configuration using the provided writer.
    */
   void serializeConfiguration(ConfigurationWriter writer);

   /**
    * Stops the specified servers.
    *
    * @param servers the list of server names to stop.
    */
   void serverStop(List<String> servers);

   /**
    * Stops the entire cluster.
    */
   void clusterStop();

   /**
    * Stops the container (cache manager and all associated services).
    */
   void containerStop();

   /**
    * @deprecated Multiple Cache Managers are not supported in the server. Use {@link #getCacheManager()} instead.
    */
   @Deprecated(forRemoval=true, since = "13.0")
   default DefaultCacheManager getCacheManager(String name) {
      DefaultCacheManager cm = getCacheManager();
      return cm.getName().equals(name) ? cm : null;
   }

   /**
    * Returns the server's {@link DefaultCacheManager}.
    */
   DefaultCacheManager getCacheManager();

   /**
    * Returns the {@link ServerStateManager} responsible for managing cluster-wide server state
    * such as ignored caches, connector status, and IP filters.
    */
   ServerStateManager getServerStateManager();

   /**
    * Returns the login configuration for the given protocol server, including authentication mode,
    * URL, realm, and client ID.
    *
    * @param protocolServer the protocol server to retrieve login configuration for.
    */
   Map<String, String> getLoginConfiguration(ProtocolServer protocolServer);

   /**
    * Returns all registered protocol servers, keyed by their name.
    */
   Map<String, ProtocolServer> getProtocolServers();

   /**
    * Returns the {@link TaskManager} for managing server tasks.
    */
   TaskManager getTaskManager();

   /**
    * Generates a diagnostic server report.
    *
    * @return a {@link CompletionStage} that completes with the path to the generated report file.
    */
   CompletionStage<Path> getServerReport();

   /**
    * Returns the {@link BackupManager} for managing cluster backup and restore operations.
    */
   BackupManager getBackupManager();

   /**
    * Returns all configured data sources, keyed by their JNDI name.
    */
   Map<String, DataSource> getDataSources();

   /**
    * Returns the path to the server's data directory.
    */
   Path getServerDataPath();

   /**
    * Returns all users grouped by security realm.
    */
   Map<String, List<Principal>> getUsers();

   /**
    * Flushes all security caches across the cluster, including authorization and realm caches.
    */
   CompletionStage<Void> flushSecurityCaches();

   /**
    * Generates a JSON overview report of the server, including cluster topology, cache features,
    * encodings, persistence stores, and connected clients.
    */
   Json overviewReport();

   /**
    * Generates a JSON security overview report of the server, including configured security realms
    * and their authentication mechanisms.
    */
   Json securityOverviewReport();

   /**
    * Returns the class loader used by this server instance.
    */
   ClassLoader getClassLoader();

   /**
    * Returns whether this server is running in a managed environment, such as a Kubernetes or OpenShift
    * cluster managed by the Infinispan Operator.
    *
    * @return {@code true} if the server is managed by an operator, {@code false} otherwise.
    * @since 16.3
    */
   default boolean isManaged() {
      return false;
   }
}
