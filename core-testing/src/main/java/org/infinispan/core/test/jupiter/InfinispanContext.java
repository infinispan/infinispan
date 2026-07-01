package org.infinispan.core.test.jupiter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Test context providing access to the Infinispan cluster and per-test caches.
 * <p>
 * Obtained via {@link InfinispanResource} injection. The context provides:
 * <ul>
 *   <li>Access to the shared {@link EmbeddedCacheManager} instances</li>
 *   <li>Per-test cache creation with automatic cleanup</li>
 *   <li>Time control via {@link ControlledTimeService}</li>
 * </ul>
 *
 * @since 16.2
 */
public class InfinispanContext {

   private final ClusterHandle cluster;
   private final String testId;
   private final int initialNodeCount;
   private final List<String> createdCaches = Collections.synchronizedList(new ArrayList<>());
   private int cacheCounter;
   private NetworkController networkController;

   InfinispanContext(ClusterHandle cluster, String testId) {
      this.cluster = cluster;
      this.testId = testId;
      this.initialNodeCount = cluster.managers().size();
   }

   /**
    * Returns the number of nodes in the cluster.
    */
   public int numNodes() {
      return cluster.managers().size();
   }

   /**
    * Returns the cache manager at the given index.
    */
   public EmbeddedCacheManager manager(int index) {
      return cluster.managers().get(index);
   }

   /**
    * Returns an unmodifiable view of all cache managers.
    */
   public List<EmbeddedCacheManager> managers() {
      return Collections.unmodifiableList(cluster.managers());
   }

   /**
    * Creates a cache with the given configuration on all nodes and returns a {@link CacheHandle}.
    * The cache is automatically destroyed after the test.
    *
    * @param configurer a consumer that configures the cache
    * @return a handle providing access to the cache on each node
    */
   public <K, V> CacheHandle<K, V> createCache(Consumer<ConfigurationBuilder> configurer) {
      String cacheName = testId + "-" + (cacheCounter++);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      configurer.accept(builder);
      for (EmbeddedCacheManager manager : cluster.managers()) {
         manager.defineConfiguration(cacheName, builder.build());
      }
      createdCaches.add(cacheName);
      return new CacheHandle<>(cluster.managers(), cacheName);
   }

   /**
    * Creates a local (non-clustered) cache with the given configuration on all nodes.
    *
    * @param configurer a consumer that configures the cache
    * @return a handle providing access to the cache on each node
    */
   public <K, V> CacheHandle<K, V> createLocalCache(Consumer<ConfigurationBuilder> configurer) {
      return createCache(configurer);
   }

   /**
    * Creates a cache with default configuration on all nodes.
    */
   public <K, V> CacheHandle<K, V> createCache() {
      return createCache(b -> {
      });
   }

   /**
    * Creates a cache from a declarative configuration string (XML, JSON, or YAML).
    * <p>
    * The string should contain a single cache element, e.g.:
    * <pre>{@code
    * var cache = ctx.<String, String>createCacheFromString("""
    *       <distributed-cache name="my-cache" mode="SYNC" owners="2">
    *          <memory max-count="1000"/>
    *       </distributed-cache>
    *       """);
    * }</pre>
    * <p>
    * The media type is auto-detected from the content. The cache name in the
    * declaration is ignored — a unique test-scoped name is assigned instead.
    *
    * @param configuration the declarative configuration string
    * @return a handle providing access to the cache on each node
    */
   public <K, V> CacheHandle<K, V> createCacheFromString(String configuration) {
      return createCacheFromString(configuration, null);
   }

   /**
    * Creates a cache from a declarative configuration string with an explicit media type.
    *
    * @param configuration the declarative configuration string
    * @param mediaType     the media type (e.g. {@link MediaType#APPLICATION_XML}), or {@code null} for auto-detection
    * @return a handle providing access to the cache on each node
    */
   public <K, V> CacheHandle<K, V> createCacheFromString(String configuration, MediaType mediaType) {
      Configuration config = parseCacheConfiguration(configuration, mediaType);
      return defineCache(config);
   }

   /**
    * Creates a cache from a declarative configuration file (XML, JSON, or YAML) on the classpath or filesystem.
    * <p>
    * The file should contain a single cache element. The format is auto-detected
    * from the file extension. The cache name in the file is ignored — a unique
    * test-scoped name is assigned instead.
    * <p>
    * Example:
    * <pre>{@code
    * var cache = ctx.<String, String>createCacheFromFile("my-cache-config.xml");
    * }</pre>
    *
    * @param filename the configuration file path
    * @return a handle providing access to the cache on each node
    */
   public <K, V> CacheHandle<K, V> createCacheFromFile(String filename) {
      ConfigurationBuilderHolder holder;
      try {
         holder = new ParserRegistry().parseFile(filename);
      } catch (IOException e) {
         throw new UncheckedIOException("Failed to parse cache configuration file: " + filename, e);
      }
      Configuration config = extractSingleCacheConfig(holder);
      return defineCache(config);
   }

   /**
    * Returns the {@link ControlledTimeService} for manual time advancement.
    *
    * @throws IllegalStateException if the cluster was not created with {@code controlledTime = true}
    */
   public ControlledTimeService timeService() {
      ControlledTimeService ts = cluster.timeService();
      if (ts == null) {
         throw new IllegalStateException(
               "ControlledTimeService is not available. Set @InfinispanCluster(controlledTime = true)");
      }
      return ts;
   }

   /**
    * Adds a new node to the cluster and waits for the cluster view to stabilize.
    * <p>
    * The new node will have access to all named cache configurations already
    * defined via the configuration file, but per-test caches created with
    * {@link #createCache} are <b>not</b> automatically defined on the new node.
    * Use the returned manager to define and start them if needed.
    *
    * @return the new cache manager
    */
   public EmbeddedCacheManager addNode() {
      return cluster.addNode();
   }

   /**
    * Stops and removes the cache manager at the given index.
    * <p>
    * After this call, node indices shift down — the node that was at
    * {@code index + 1} becomes {@code index}. The cluster view is
    * automatically awaited if more than one node remains.
    *
    * @param index the node index to kill
    */
   public void kill(int index) {
      cluster.kill(index);
   }

   /**
    * Stops the node at the given index and restarts it, preserving its
    * persistent state. The node recovers its identity and data from disk.
    * <p>
    * Requires {@code @InfinispanCluster(globalState = true)}.
    * <p>
    * After restart, the node index remains the same. Previously obtained
    * cache references for this node become invalid — use
    * {@link CacheHandle#on(int)} to get fresh references.
    *
    * @param index the node index to restart
    * @return the new cache manager
    */
   public EmbeddedCacheManager restart(int index) {
      return cluster.restart(index);
   }

   /**
    * Waits for the cluster view to stabilize with the expected number of members
    * on all cache managers.
    */
   public void waitForClusterToForm() {
      cluster.waitForClusterToForm();
   }

   /**
    * Returns a {@link NetworkController} for injecting network failures.
    * <p>
    * The controller allows isolating nodes, creating network partitions,
    * and merging them back. All network modifications are automatically
    * reverted after each test.
    *
    * @throws IllegalStateException if the cluster has fewer than 2 nodes
    */
   public NetworkController network() {
      if (cluster.managers().size() < 2) {
         throw new IllegalStateException("Network control requires at least 2 nodes");
      }
      if (networkController == null) {
         networkController = new NetworkController(cluster.managers());
      }
      return networkController;
   }

   private <K, V> CacheHandle<K, V> defineCache(Configuration config) {
      String cacheName = testId + "-" + (cacheCounter++);
      for (EmbeddedCacheManager manager : cluster.managers()) {
         manager.defineConfiguration(cacheName, config);
      }
      createdCaches.add(cacheName);
      return new CacheHandle<>(cluster.managers(), cacheName);
   }

   static Configuration parseCacheConfiguration(String configuration, MediaType mediaType) {
      ParserRegistry parser = new ParserRegistry();
      ConfigurationBuilderHolder holder = mediaType != null
            ? parser.parse(configuration, mediaType)
            : parser.parse(configuration);
      return extractSingleCacheConfig(holder);
   }

   static Configuration extractSingleCacheConfig(ConfigurationBuilderHolder holder) {
      Map<String, ConfigurationBuilder> builders = holder.getNamedConfigurationBuilders();
      if (builders.isEmpty()) {
         throw new IllegalArgumentException(
               "No cache configuration found in the provided input");
      }
      if (builders.size() > 1) {
         throw new IllegalArgumentException(
               "Expected a single cache configuration but found " + builders.size() +
                     ": " + builders.keySet());
      }
      return builders.values().iterator().next().build();
   }

   void cleanup() {
      // Reset network modifications first, before cache removal
      if (networkController != null) {
         networkController.reset();
         networkController = null;
      }
      // Remove per-test caches
      for (String cacheName : createdCaches) {
         for (EmbeddedCacheManager manager : cluster.managers()) {
            try {
               manager.administration().removeCache(cacheName);
            } catch (Exception ignored) {
            }
         }
      }
      createdCaches.clear();
      cacheCounter = 0;

      // Restore cluster to original size
      int currentSize = cluster.managers().size();
      // Kill extra nodes that were added during the test
      while (cluster.managers().size() > initialNodeCount) {
         cluster.kill(cluster.managers().size() - 1);
      }
      // Re-add nodes that were killed during the test
      while (cluster.managers().size() < initialNodeCount) {
         cluster.addNode();
      }
   }

   /**
    * A handle to a cache defined on one or more nodes.
    * <p>
    * Use {@link #on(int)} to access the cache on a specific node.
    */
   public static class CacheHandle<K, V> {
      private final List<EmbeddedCacheManager> managers;
      private final String cacheName;

      CacheHandle(List<EmbeddedCacheManager> managers, String cacheName) {
         this.managers = managers;
         this.cacheName = cacheName;
      }

      /**
       * Returns the cache instance on the given node.
       */
      public Cache<K, V> on(int nodeIndex) {
         return managers.get(nodeIndex).getCache(cacheName);
      }

      /**
       * Returns the cache on node 0. Convenience for single-node tests.
       */
      public Cache<K, V> cache() {
         return on(0);
      }

      /**
       * Returns the cache name.
       */
      public String name() {
         return cacheName;
      }
   }
}
