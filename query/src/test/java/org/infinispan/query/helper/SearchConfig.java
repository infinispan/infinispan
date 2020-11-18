package org.infinispan.query.helper;

import org.hibernate.search.backend.lucene.cfg.LuceneIndexSettings;

public class SearchConfig {

   // Engine level properties:
   public static final String ERROR_HANDLER = "hibernate.search.background_failure_handler";

   // Backend level properties:
   public static final String DIRECTORY_TYPE = "directory.type";
   public static final String HEAP = "local-heap";
   public static final String FILE = "local-filesystem";

   public static final String DIRECTORY_ROOT = "directory.root";

   public static final String THREAD_POOL_SIZE = "thread_pool.size";

   public static final String QUEUE_COUNT = ".indexing.queue_count";
   public static final String QUEUE_SIZE = ".indexing.queue_size";

   public static final String COMMIT_INTERVAL = "." + LuceneIndexSettings.IO_COMMIT_INTERVAL;
   public static final String IO_MERGE_FACTOR = "." + LuceneIndexSettings.IO_MERGE_FACTOR;
   public static final String IO_MERGE_MAX_SIZE = "." + LuceneIndexSettings.IO_MERGE_MAX_SIZE;
   public static final String IO_WRITER_RAM_BUFFER_SIZE = "." + LuceneIndexSettings.IO_WRITER_RAM_BUFFER_SIZE;

   public static final String IO_STRATEGY = "." + LuceneIndexSettings.IO_STRATEGY;
   public static final String NEAR_REAL_TIME = "near-real-time";

   public static final String SHARDING_STRATEGY = ".sharding.strategy";
   public static final String HASH = "hash";

   public static final String NUMBER_OF_SHARDS = ".sharding.number_of_shards";

   private SearchConfig() {
   }
}
