package org.infinispan.query.core.stats;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A snapshot of {@link IndexStatistics}.
 *
 * @since 12.0
 * @see IndexStatistics#computeSnapshot()
 */
public interface IndexStatisticsSnapshot extends IndexStatistics, JsonSerialization {

    @Override
    default Set<String> indexedEntities() {
        return indexInfos().keySet();
    }

    @Override
    default CompletionStage<Map<String, IndexInfo>> computeIndexInfos() {
        return CompletableFuture.completedFuture(indexInfos());
    }

    @Override
    default CompletionStage<IndexStatisticsSnapshot> computeSnapshot() {
        return CompletableFuture.completedFuture(this);
    }

    /**
     * @return The {@link IndexInfo} for each indexed entity configured in the cache. The name of the entity is
     * either the class name annotated with @Index, or the protobuf Message name.
     */
    Map<String, IndexInfo> indexInfos();

    /**
     * Merge with another {@link IndexStatisticsSnapshot}.
     *
     * @return self
     */
    IndexStatisticsSnapshot merge(IndexStatisticsSnapshot other);

}
