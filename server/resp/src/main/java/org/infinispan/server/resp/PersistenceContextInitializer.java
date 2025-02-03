package org.infinispan.server.resp;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.server.resp.filter.GlobMatchFilterConverter;
import org.infinispan.server.resp.filter.RespTypeFilterConverter;
import org.infinispan.server.resp.hll.HyperLogLog;
import org.infinispan.server.resp.hll.internal.CompactSet;
import org.infinispan.server.resp.hll.internal.ExplicitSet;
import org.infinispan.server.resp.json.JsonBucket;

@ProtoSchema(
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = {
            HyperLogLog.class,
            ExplicitSet.class,
            CompactSet.class,
            GlobMatchFilterConverter.class,
            RespTypeFilterConverter.class,
            JsonBucket.class
      },
      schemaFileName = "persistence.resp.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.resp",
      service = false
)
public interface PersistenceContextInitializer extends SerializationContextInitializer { }
