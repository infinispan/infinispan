package org.infinispan.query.impl.config;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.hibernate.search.backend.lucene.analysis.LuceneAnalysisConfigurer;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.cache.IndexMergeConfiguration;
import org.infinispan.configuration.cache.IndexReaderConfiguration;
import org.infinispan.configuration.cache.IndexShardingConfiguration;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.IndexWriterConfiguration;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.search.mapper.mapping.impl.CompositeAnalysisConfigurer;

/**
 * Extracts Hibernate Search native configuration properties from a {@link IndexingConfiguration}.
 *
 * @since 12.0
 */
public class SearchPropertyExtractor {

   private static final String BACKEND_PREFIX = "hibernate.search.backend.";
   private static final String DIRECTORY_ROOT_KEY = BACKEND_PREFIX + "directory.root";
   private static final String DIRECTORY_PROVIDER_KEY = BACKEND_PREFIX + "directory.type";
   private static final String LOCAL_HEAP_DIRECTORY_PROVIDER = "local-heap";
   private static final String FS_PROVIDER = "local-filesystem";

   private static final String REFRESH_INTERVAL_KEY = "hibernate.search.backend.io.refresh_interval";

   private static final String IO_PREFIX = "hibernate.search.backend.io.";
   private static final String COMMIT_INTERVAL_KEY = IO_PREFIX + "commit_interval";
   private static final String RAM_BUFFER_KEY = IO_PREFIX + "writer.ram_buffer_size";
   private static final String MAX_BUFFER_DOCS_KEY = IO_PREFIX + "writer.max_buffered_docs";
   private static final String LOW_LEVEL_TRACE_KEY = IO_PREFIX + "writer.infostream";
   private static final String QUEUE_COUNT_KEY = BACKEND_PREFIX + "indexing.queue_count";
   private static final String QUEUE_SIZE_KEY = BACKEND_PREFIX + "indexing.queue_size";
   private static final String THREAD_POOL_KEY = BACKEND_PREFIX + "thread_pool.size";

   private static final String KEY_PREFIX = "hibernate.search.backend.io.merge";
   private static final String MAX_DOCS_KEY = KEY_PREFIX + ".max_docs";
   private static final String FACTOR_KEY = KEY_PREFIX + ".factor";
   private static final String MIN_SIZE_KEY = KEY_PREFIX + ".min_size";
   private static final String MAX_SIZE_KEY = KEY_PREFIX + ".max_size";
   private static final String MAX_FORCED_SIZE_KEY = KEY_PREFIX + ".max_forced_size";
   private static final String CALIBRATE_BY_DELETES_KEY = KEY_PREFIX + ".calibrate_by_deletes";

   private static final String ANALYSIS_CONFIGURER_PROPERTY_NAME = "analysis.configurer";

   public static Map<String, Object> extractProperties(GlobalConfiguration globalConfiguration,
                                                       String cacheName,
                                                       IndexingConfiguration configuration,
                                                       ClassLoader aggregatedClassLoader) {
      Map<String, Object> props = new LinkedHashMap<>();

      // load LuceneAnalysisDefinitionProvider from classpath
      Collection<LuceneAnalysisConfigurer> analyzerDefProviders = ServiceFinder.load(LuceneAnalysisConfigurer.class, aggregatedClassLoader);
      if (analyzerDefProviders.size() == 1) {
         props.put(ANALYSIS_CONFIGURER_PROPERTY_NAME, analyzerDefProviders.iterator().next());
      } else if (!analyzerDefProviders.isEmpty()) {
         props.put(ANALYSIS_CONFIGURER_PROPERTY_NAME, new CompositeAnalysisConfigurer(analyzerDefProviders));
      }

      if (!configuration.enabled()) {
         return Collections.unmodifiableMap(props);
      }

      IndexStorage storage = configuration.storage();
      if (storage.equals(IndexStorage.LOCAL_HEAP)) {
         props.put(DIRECTORY_PROVIDER_KEY, LOCAL_HEAP_DIRECTORY_PROVIDER);
      } else {
         props.put(DIRECTORY_PROVIDER_KEY, FS_PROVIDER);
         Path location = getIndexLocation(globalConfiguration, configuration.path(), cacheName);
         props.put(DIRECTORY_ROOT_KEY, location.toFile().getPath());
      }
      IndexReaderConfiguration readerConfiguration = configuration.reader();
      long refreshInterval = readerConfiguration.getRefreshInterval();
      if (refreshInterval != 0) {
         props.put(REFRESH_INTERVAL_KEY, refreshInterval);
      }

      IndexWriterConfiguration writerConfiguration = configuration.writer();

      Integer commitInterval = writerConfiguration.getCommitInterval();
      if (commitInterval != null) {
         props.put(COMMIT_INTERVAL_KEY, commitInterval);
      }
      Integer threadPoolSize = writerConfiguration.getThreadPoolSize();
      if (threadPoolSize != null) {
         props.put(THREAD_POOL_KEY, threadPoolSize);
      }
      Integer queueCount = writerConfiguration.getQueueCount();
      if (queueCount != null) {
         props.put(QUEUE_COUNT_KEY, queueCount);
      }
      Integer queueSize = writerConfiguration.getQueueSize();
      if (queueSize != null) {
         props.put(QUEUE_SIZE_KEY, queueSize);
      }
      Integer ramBufferSize = writerConfiguration.getRamBufferSize();
      if (ramBufferSize != null) {
         props.put(RAM_BUFFER_KEY, ramBufferSize);
      }
      Integer maxBufferedDocs = writerConfiguration.getMaxBufferedEntries();
      if (maxBufferedDocs != null) {
         props.put(MAX_BUFFER_DOCS_KEY, maxBufferedDocs);
      }
      Boolean lowLevelTrace = writerConfiguration.isLowLevelTrace();
      if (lowLevelTrace) {
         props.put(LOW_LEVEL_TRACE_KEY, true);
      }

      IndexMergeConfiguration mergeConfiguration = writerConfiguration.merge();
      Integer maxDocs = mergeConfiguration.maxEntries();
      if (maxDocs != null) {
         props.put(MAX_DOCS_KEY, maxDocs);
      }
      Integer minSize = mergeConfiguration.minSize();
      if (minSize != null) {
         props.put(MIN_SIZE_KEY, minSize);
      }
      Integer maxSize = mergeConfiguration.maxSize();
      if (maxSize != null) {
         props.put(MAX_SIZE_KEY, maxSize);
      }
      Integer factor = mergeConfiguration.factor();
      if (factor != null) {
         props.put(FACTOR_KEY, factor);
      }
      Integer maxForcedSize = mergeConfiguration.maxForcedSize();
      if (maxForcedSize != null) {
         props.put(MAX_FORCED_SIZE_KEY, maxForcedSize);
      }
      Boolean calibrateByDeletes = mergeConfiguration.calibrateByDeletes();
      if (calibrateByDeletes != null) {
         props.put(CALIBRATE_BY_DELETES_KEY, calibrateByDeletes);
      }

      IndexShardingConfiguration sharding = configuration.sharding();
      if (sharding != null) {
         Integer numberOfShards = sharding.getShards();
         if (numberOfShards > 1) {
            props.put("sharding.strategy", "hash"); // the only strategy supported at the moment
            props.put("sharding.number_of_shards", numberOfShards);
         }
      }

      return Collections.unmodifiableMap(props);
   }

   public static Path getIndexLocation(GlobalConfiguration globalConfiguration, String location, String cacheName) {
      GlobalStateConfiguration globalState = globalConfiguration.globalState();
      Path persistentLocation = Paths.get(globalState.persistentLocation());

      if (location == null) return persistentLocation.resolve(cacheName);

      Path path = Paths.get(location);

      return path.isAbsolute() ? path : persistentLocation.resolve(path);
   }
}
