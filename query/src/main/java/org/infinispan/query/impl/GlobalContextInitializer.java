package org.infinispan.query.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

@ProtoSchema(
      allowNullFields = true,
      dependsOn = {
            org.infinispan.marshall.protostream.impl.GlobalContextInitializer.class,
            org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
            org.infinispan.objectfilter.impl.GlobalContextInitializer.class,
            org.infinispan.protostream.types.java.CommonTypes.class,
            org.infinispan.query.core.stats.impl.PersistenceContextInitializer.class
      },
      includeClasses = {
            org.infinispan.query.clustered.ClusteredQueryOperation.class,
            org.infinispan.query.clustered.NodeTopDocs.class,
            org.infinispan.query.clustered.SegmentsClusteredQueryCommand.class,
            org.infinispan.query.clustered.QueryResponse.class,
            org.infinispan.query.clustered.commandworkers.CQCommandType.class,
            org.infinispan.query.impl.QueryDefinition.class,
            org.infinispan.query.impl.massindex.IndexWorker.class,
            org.infinispan.query.impl.protostream.adapters.HibernatePojoRawTypeIdentifierAdapter.class,
            org.infinispan.query.impl.protostream.adapters.LuceneBytesRefAdapter.class,
            org.infinispan.query.impl.protostream.adapters.LuceneFieldDocAdapter.class,
            org.infinispan.query.impl.protostream.adapters.LuceneSortAdapter.class,
            org.infinispan.query.impl.protostream.adapters.LuceneSortFieldAdapter.class,
            org.infinispan.query.impl.protostream.adapters.LuceneSortFieldTypeAdapter.class,
            org.infinispan.query.impl.protostream.adapters.LuceneScoreDocAdapter.class,
            org.infinispan.query.impl.protostream.adapters.LuceneTopDocsAdapter.class,
            org.infinispan.query.impl.protostream.adapters.LuceneTopFieldDocsAdapter.class,
            org.infinispan.query.impl.protostream.adapters.LuceneTotalHitsAdapter.class,
            org.infinispan.query.impl.protostream.adapters.LuceneTotalHitsRelationAdapter.class,
      },
      schemaFileName = "global.query.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.global.query",
      service = false,
      syntax = ProtoSyntax.PROTO3

)
public interface GlobalContextInitializer extends SerializationContextInitializer {
}
