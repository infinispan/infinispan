package org.infinispan.core.test.jupiter;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.core.test.jupiter.transport.TestTransport;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * Manages a cluster of {@link EmbeddedCacheManager} instances.
 * <p>
 * Created once per test class and destroyed when no longer needed.
 * Each cluster is isolated via a unique JGroups cluster name and LOCAL_PING discovery.
 * <p>
 * Supports dynamic membership changes via {@link #addNode()} and {@link #kill(int)}.
 *
 * @since 16.2
 */
class ClusterHandle implements AutoCloseable {

   private final List<EmbeddedCacheManager> managers;
   private final ControlledTimeService timeService;
   private final String clusterName;
   private final String transportStack;
   private final String configFile;
   private final List<SerializationContextInitializer> contextInitializers;
   private final boolean globalState;
   private final Path stateBaseDir;
   private final Map<Integer, Path> nodeStateDirs;
   private final int portOffset;
   private int nodeCounter;

   ClusterHandle(int numNodes, boolean controlledTime, String transportStack, String configFile,
                 List<SerializationContextInitializer> contextInitializers, boolean globalState) {
      this.managers = new ArrayList<>(numNodes);
      this.timeService = controlledTime ? new ControlledTimeService() : null;
      this.transportStack = transportStack;
      this.configFile = configFile;
      this.contextInitializers = contextInitializers;
      this.globalState = globalState;
      this.nodeStateDirs = new HashMap<>();
      this.clusterName = "ISPN-test-" + System.nanoTime();
      this.portOffset = numNodes > 1 ? TestTransport.allocatePortOffset() : 0;

      if (globalState) {
         try {
            this.stateBaseDir = Files.createTempDirectory("ispn-test-state-");
         } catch (IOException e) {
            throw new UncheckedIOException("Failed to create state directory", e);
         }
      } else {
         this.stateBaseDir = null;
      }

      for (int i = 0; i < numNodes; i++) {
         managers.add(createManager(numNodes > 1));
      }

      if (numNodes > 1) {
         waitForClusterToForm();
      }
   }

   /**
    * Creates and starts a new node, adding it to the cluster.
    *
    * @return the new cache manager
    */
   EmbeddedCacheManager addNode() {
      EmbeddedCacheManager manager = createManager(true);
      managers.add(manager);
      waitForClusterToForm();
      return manager;
   }

   /**
    * Stops and removes the cache manager at the given index.
    *
    * @param index the node index to kill
    */
   void kill(int index) {
      EmbeddedCacheManager manager = managers.remove(index);
      try {
         manager.stop();
      } catch (Exception ignored) {
      }
      if (!managers.isEmpty() && managers.size() > 1) {
         waitForClusterToForm();
      }
   }

   /**
    * Stops the node at the given index and restarts it, preserving its
    * persistent state directory. Requires {@code globalState = true}.
    *
    * @param index the node index to restart
    * @return the new cache manager
    */
   EmbeddedCacheManager restart(int index) {
      if (!globalState) {
         throw new IllegalStateException(
               "restart() requires globalState = true. Set @InfinispanCluster(globalState = true)");
      }

      EmbeddedCacheManager old = managers.get(index);

      // Find the node ID that was assigned to this manager
      int nodeId = findNodeId(old);

      try {
         old.stop();
      } catch (Exception ignored) {
      }

      // Recreate with the same state directory
      EmbeddedCacheManager manager = createManagerWithNodeId(true, nodeId);
      managers.set(index, manager);

      if (managers.size() > 1) {
         waitForClusterToForm();
      }

      return manager;
   }

   private int findNodeId(EmbeddedCacheManager manager) {
      String persistentLocation = manager.getCacheManagerConfiguration()
            .globalState().persistentLocation();
      for (var entry : nodeStateDirs.entrySet()) {
         if (entry.getValue().toString().equals(persistentLocation)) {
            return entry.getKey();
         }
      }
      throw new IllegalStateException("Could not find node ID for manager at " + persistentLocation);
   }

   List<EmbeddedCacheManager> managers() {
      return managers;
   }

   ControlledTimeService timeService() {
      return timeService;
   }

   void waitForClusterToForm() {
      int expectedSize = managers.size();
      if (expectedSize <= 1) return;
      await().atMost(30, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
               for (EmbeddedCacheManager manager : managers) {
                  int viewSize = manager.getMembers() != null ? manager.getMembers().size() : 0;
                  if (viewSize != expectedSize) {
                     throw new AssertionError("Expected " + expectedSize + " members but got " + viewSize);
                  }
               }
            });
   }

   boolean globalState() {
      return globalState;
   }

   @Override
   public void close() {
      for (int i = managers.size() - 1; i >= 0; i--) {
         try {
            managers.get(i).stop();
         } catch (Exception ignored) {
         }
      }
      managers.clear();

      // Clean up state directories
      if (stateBaseDir != null) {
         deleteDirectory(stateBaseDir);
      }
   }

   private EmbeddedCacheManager createManager(boolean clustered) {
      return createManagerWithNodeId(clustered, nodeCounter++);
   }

   private EmbeddedCacheManager createManagerWithNodeId(boolean clustered, int nodeId) {
      ConfigurationBuilderHolder holder = parseConfiguration(configFile);
      GlobalConfigurationBuilder gcb = holder.getGlobalConfigurationBuilder();

      if (clustered) {
         TestTransport.configure(gcb, clusterName, transportStack, nodeId, portOffset);
      } else {
         gcb.nonClusteredDefault();
      }

      gcb.defaultCacheName(null);

      for (SerializationContextInitializer sci : contextInitializers) {
         gcb.serialization().addContextInitializer(sci);
      }

      if (globalState) {
         Path nodeDir = stateBaseDir.resolve("node-" + nodeId);
         try {
            Files.createDirectories(nodeDir);
         } catch (IOException e) {
            throw new UncheckedIOException("Failed to create node state directory", e);
         }
         nodeStateDirs.put(nodeId, nodeDir);
         gcb.globalState().enable()
               .persistentLocation(nodeDir.toString())
               .sharedPersistentLocation(nodeDir.toString())
               .temporaryLocation(nodeDir.resolve("tmp").toString());
      }

      EmbeddedCacheManager manager = new DefaultCacheManager(holder, true);

      if (timeService != null) {
         replaceTimeService(manager, timeService);
      }

      return manager;
   }

   private static ConfigurationBuilderHolder parseConfiguration(String configFile) {
      if (configFile == null || configFile.isEmpty()) {
         return new ConfigurationBuilderHolder();
      }
      try {
         return new ParserRegistry().parseFile(configFile);
      } catch (IOException e) {
         throw new UncheckedIOException("Failed to parse configuration file: " + configFile, e);
      }
   }

   private static void replaceTimeService(EmbeddedCacheManager manager, TimeService timeService) {
      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(manager);
      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      bcr.replaceComponent(TimeService.class.getName(), timeService, true);
      gcr.rewire();
   }

   private static void deleteDirectory(Path dir) {
      try (var stream = Files.walk(dir)) {
         stream.sorted(Comparator.reverseOrder())
               .forEach(path -> {
                  try {
                     Files.deleteIfExists(path);
                  } catch (IOException ignored) {
                  }
               });
      } catch (IOException ignored) {
      }
   }
}
