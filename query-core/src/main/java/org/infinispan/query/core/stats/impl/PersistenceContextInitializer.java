package org.infinispan.query.core.stats.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.query.core.stats.IndexInfo;

/**
 * @since 12.0
 */
@ProtoSchema(
      includeClasses = {
            QueryMetrics.class,
            LocalQueryStatistics.class,
            IndexStatisticsSnapshotImpl.class,
            SearchStatisticsSnapshotImpl.class,
            IndexInfo.class,
            IndexEntry.class,
            StatsTask.class
      },
      schemaFileName = "persistence.query.core.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.query.core",
      service = false
)
public interface PersistenceContextInitializer extends SerializationContextInitializer {
}
