package org.infinispan.query.core.stats;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 *
 * Exposes index statistics for a particular cache.
 *
 * @since 12.0
 */
public interface IndexStatistics {

   /**
    * @return The name of all indexed entities configured in the cache. The name of the entity is
    * either the class name annotated with @Index, or the protobuf Message name.
    */
   Set<String> indexedEntities();

   /**
    * @return The {@link IndexInfo} for each indexed entity configured in the cache. The name of the entity is
    * either the class name annotated with @Index, or the protobuf Message name.
    */
   CompletionStage<Map<String, IndexInfo>> computeIndexInfos();

   boolean reindexing();

   int genericIndexingFailures();

   int entityIndexingFailures();

   CompletionStage<IndexStatisticsSnapshot> computeSnapshot();

}
