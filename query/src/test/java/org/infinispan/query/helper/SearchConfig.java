package org.infinispan.query.helper;

public class SearchConfig {

   // Engine level properties:
   public static final String ERROR_HANDLER = "hibernate.search.background_failure_handler";

   // Backend level properties:
   public static final String DIRECTORY_TYPE = "directory.type";
   public static final String HEAP = "local-heap";
   public static final String FILE = "local-filesystem";

   public static final String DIRECTORY_ROOT = "directory.root";

   public static final String THREAD_POOL_SIZE = "thread_pool.size";

   private static final String DEFAULT_INDEX_PREFIX = "index_defaults";

   public static final String QUEUE_COUNT = DEFAULT_INDEX_PREFIX + ".indexing.queue_count";

   public static final String QUEUE_SIZE = DEFAULT_INDEX_PREFIX + ".indexing.queue_size";

   public static final String COMMIT_INTERVAL = DEFAULT_INDEX_PREFIX + ".io.commit_interval";

   public static final String REFRESH_INTERVAL = DEFAULT_INDEX_PREFIX + ".io.refresh_interval";

   public static final String SHARDING_STRATEGY = DEFAULT_INDEX_PREFIX + ".sharding.strategy";
   public static final String HASH = "hash";

   public static final String NUMBER_OF_SHARDS = DEFAULT_INDEX_PREFIX + ".sharding.number_of_shards";

   private SearchConfig() {
   }
}
