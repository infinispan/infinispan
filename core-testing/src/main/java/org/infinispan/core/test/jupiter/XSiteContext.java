package org.infinispan.core.test.jupiter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Test context for cross-site tests. Provides access to sites, cache managers,
 * and per-test cache creation with automatic backup wiring.
 * <p>
 * Obtained via {@link InfinispanResource} injection in {@link InfinispanXSite} tests.
 *
 * @since 16.2
 */
public class XSiteContext {

   private final XSiteClusterHandle cluster;
   private final String testId;
   private final List<String> createdCaches = Collections.synchronizedList(new ArrayList<>());
   private int cacheCounter;
   private SiteController siteController;

   XSiteContext(XSiteClusterHandle cluster, String testId) {
      this.cluster = cluster;
      this.testId = testId;
   }

   /**
    * Returns the names of all sites in the topology.
    */
   public List<String> siteNames() {
      return List.copyOf(cluster.sites().keySet());
   }

   /**
    * Returns the cache manager at the given index in the named site.
    */
   public EmbeddedCacheManager manager(String site, int index) {
      return cluster.site(site).managers().get(index);
   }

   /**
    * Returns the first cache manager of the named site.
    */
   public EmbeddedCacheManager manager(String site) {
      return manager(site, 0);
   }

   /**
    * Returns all cache managers for the named site.
    */
   public List<EmbeddedCacheManager> managers(String site) {
      return Collections.unmodifiableList(cluster.site(site).managers());
   }

   /**
    * Returns the number of nodes in the named site.
    */
   public int numNodes(String site) {
      return cluster.site(site).numNodes();
   }

   /**
    * Creates a cache across all sites using a fluent builder.
    * <p>
    * Example — bidirectional sync backup between all sites:
    * <pre>{@code
    * var cache = ctx.createCache(c -> c
    *       .cacheMode(CacheMode.DIST_SYNC)
    *       .backups(BackupStrategy.SYNC));
    * }</pre>
    * <p>
    * Example — asymmetric backup:
    * <pre>{@code
    * var cache = ctx.createCache(c -> c
    *       .cacheMode(CacheMode.DIST_SYNC)
    *       .backup("LON", "NYC", BackupStrategy.SYNC)
    *       .backup("NYC", "LON", BackupStrategy.ASYNC));
    * }</pre>
    * <p>
    * Example — per-site configuration:
    * <pre>{@code
    * var cache = ctx.createCache(c -> c
    *       .cacheMode(CacheMode.DIST_SYNC)
    *       .backups(BackupStrategy.SYNC)
    *       .site("LON", b -> b.expiration().lifespan(1, TimeUnit.HOURS)));
    * }</pre>
    */
   public <K, V> XSiteCacheHandle<K, V> createCache(Consumer<XSiteCacheBuilder> configurer) {
      String cacheName = testId + "-" + (cacheCounter++);
      XSiteCacheBuilder builder = new XSiteCacheBuilder(siteNames());
      configurer.accept(builder);

      // Define configuration on all sites first
      for (String siteName : siteNames()) {
         ConfigurationBuilder cb = builder.buildFor(siteName);
         for (EmbeddedCacheManager manager : cluster.site(siteName).managers()) {
            manager.defineConfiguration(cacheName, cb.build());
         }
      }

      // Start the cache on all nodes so backup targets are ready
      for (String siteName : siteNames()) {
         for (EmbeddedCacheManager manager : cluster.site(siteName).managers()) {
            manager.getCache(cacheName);
         }
      }

      createdCaches.add(cacheName);
      return new XSiteCacheHandle<>(cluster, cacheName);
   }

   /**
    * Creates a cache on all sites from a declarative configuration string (XML, JSON, or YAML).
    * <p>
    * The string should contain a single cache element with backup definitions, e.g.:
    * <pre>{@code
    * var cache = ctx.<String, String>createCacheFromString("""
    *       <distributed-cache name="ignored" mode="SYNC">
    *          <backups>
    *             <backup site="NYC" strategy="SYNC"/>
    *          </backups>
    *       </distributed-cache>
    *       """);
    * }</pre>
    * <p>
    * The same configuration is applied to all sites. The cache name in the
    * declaration is ignored — a unique test-scoped name is assigned.
    *
    * @param configuration the declarative configuration string
    * @return a handle providing access to the cache on each site and node
    */
   public <K, V> XSiteCacheHandle<K, V> createCacheFromString(String configuration) {
      return createCacheFromString(configuration, null);
   }

   /**
    * Creates a cache on all sites from a declarative configuration string with an explicit media type.
    *
    * @param configuration the declarative configuration string
    * @param mediaType     the media type, or {@code null} for auto-detection
    * @return a handle providing access to the cache on each site and node
    */
   public <K, V> XSiteCacheHandle<K, V> createCacheFromString(String configuration, MediaType mediaType) {
      Configuration config = InfinispanContext.parseCacheConfiguration(configuration, mediaType);
      return defineAndStartCache(config);
   }

   /**
    * Creates a cache on all sites from a declarative configuration file (XML, JSON, or YAML).
    * <p>
    * The file should contain a single cache element. The format is auto-detected
    * from the file extension. The cache name is ignored — a unique test-scoped
    * name is assigned.
    *
    * @param filename the configuration file path
    * @return a handle providing access to the cache on each site and node
    */
   public <K, V> XSiteCacheHandle<K, V> createCacheFromFile(String filename) {
      ConfigurationBuilderHolder holder;
      try {
         holder = new ParserRegistry().parseFile(filename);
      } catch (IOException e) {
         throw new UncheckedIOException("Failed to parse cache configuration file: " + filename, e);
      }
      Configuration config = InfinispanContext.extractSingleCacheConfig(holder);
      return defineAndStartCache(config);
   }

   private <K, V> XSiteCacheHandle<K, V> defineAndStartCache(Configuration config) {
      String cacheName = testId + "-" + (cacheCounter++);
      for (String siteName : siteNames()) {
         for (EmbeddedCacheManager manager : cluster.site(siteName).managers()) {
            manager.defineConfiguration(cacheName, config);
         }
      }
      // Start the cache on all nodes so backup targets are ready
      for (String siteName : siteNames()) {
         for (EmbeddedCacheManager manager : cluster.site(siteName).managers()) {
            manager.getCache(cacheName);
         }
      }
      createdCaches.add(cacheName);
      return new XSiteCacheHandle<>(cluster, cacheName);
   }

   /**
    * Returns a {@link SiteController} for injecting cross-site failures.
    * <p>
    * The controller allows disconnecting/reconnecting sites at the network level
    * and taking backup sites offline/online at the application level.
    * All modifications are automatically reverted after each test.
    */
   public SiteController sites() {
      if (siteController == null) {
         siteController = new SiteController(cluster);
      }
      return siteController;
   }

   /**
    * Returns the {@link ControlledTimeService} for manual time advancement.
    *
    * @throws IllegalStateException if controlledTime was not enabled
    */
   public ControlledTimeService timeService() {
      ControlledTimeService ts = cluster.timeService();
      if (ts == null) {
         throw new IllegalStateException(
               "ControlledTimeService is not available. Set @InfinispanXSite(controlledTime = true)");
      }
      return ts;
   }

   void cleanup() {
      if (siteController != null) {
         siteController.reset();
         siteController = null;
      }
      for (String cacheName : createdCaches) {
         for (XSiteClusterHandle.SiteInfo info : cluster.sites().values()) {
            for (EmbeddedCacheManager manager : info.managers()) {
               try {
                  manager.administration().removeCache(cacheName);
               } catch (Exception ignored) {
               }
            }
         }
      }
      createdCaches.clear();
      cacheCounter = 0;
   }

   /**
    * A handle providing access to a cache across multiple sites.
    */
   public static class XSiteCacheHandle<K, V> {
      private final XSiteClusterHandle cluster;
      private final String cacheName;

      XSiteCacheHandle(XSiteClusterHandle cluster, String cacheName) {
         this.cluster = cluster;
         this.cacheName = cacheName;
      }

      /**
       * Returns the cache on the given site and node.
       */
      public Cache<K, V> on(String site, int nodeIndex) {
         return cluster.site(site).managers().get(nodeIndex).getCache(cacheName);
      }

      /**
       * Returns the cache on node 0 of the given site.
       */
      public Cache<K, V> on(String site) {
         return on(site, 0);
      }

      /**
       * Returns the cache name.
       */
      public String name() {
         return cacheName;
      }
   }

   /**
    * Fluent builder for cross-site cache configuration.
    */
   public static class XSiteCacheBuilder {
      private final List<String> siteNames;
      private final ConfigurationBuilder baseConfig = new ConfigurationBuilder();
      private final List<BackupDef> backups = new ArrayList<>();
      private final List<SiteOverride> siteOverrides = new ArrayList<>();

      XSiteCacheBuilder(List<String> siteNames) {
         this.siteNames = siteNames;
      }

      /**
       * Sets the cache mode for all sites.
       */
      public XSiteCacheBuilder cacheMode(CacheMode mode) {
         baseConfig.clustering().cacheMode(mode);
         return this;
      }

      /**
       * Applies base configuration to all sites.
       */
      public XSiteCacheBuilder config(Consumer<ConfigurationBuilder> configurer) {
         configurer.accept(baseConfig);
         return this;
      }

      /**
       * Adds bidirectional backups between all pairs of sites with the given strategy.
       */
      public XSiteCacheBuilder backups(BackupStrategy strategy) {
         for (String from : siteNames) {
            for (String to : siteNames) {
               if (!from.equals(to)) {
                  backups.add(new BackupDef(from, to, strategy));
               }
            }
         }
         return this;
      }

      /**
       * Adds a directional backup from one site to another.
       */
      public XSiteCacheBuilder backup(String fromSite, String toSite, BackupStrategy strategy) {
         backups.add(new BackupDef(fromSite, toSite, strategy));
         return this;
      }

      /**
       * Applies additional configuration to a specific site.
       */
      public XSiteCacheBuilder site(String siteName, Consumer<ConfigurationBuilder> configurer) {
         siteOverrides.add(new SiteOverride(siteName, configurer));
         return this;
      }

      ConfigurationBuilder buildFor(String siteName) {
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.read(baseConfig.build());

         // Add backup definitions for this site
         for (BackupDef def : backups) {
            if (def.from.equals(siteName)) {
               cb.sites().addBackup()
                     .site(def.to)
                     .strategy(def.strategy);
            }
         }

         // Apply site-specific overrides
         for (SiteOverride override : siteOverrides) {
            if (override.siteName.equals(siteName)) {
               override.configurer.accept(cb);
            }
         }

         return cb;
      }

      private record BackupDef(String from, String to, BackupStrategy strategy) {
      }

      private record SiteOverride(String siteName, Consumer<ConfigurationBuilder> configurer) {
      }
   }
}
