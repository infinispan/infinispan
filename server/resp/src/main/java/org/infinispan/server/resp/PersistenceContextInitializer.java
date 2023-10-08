package org.infinispan.server.resp;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.server.resp.hll.HyperLogLog;
import org.infinispan.server.resp.hll.internal.CompactSet;
import org.infinispan.server.resp.hll.internal.ExplicitSet;

@ProtoSchema(
      dependsOn = org.infinispan.marshall.persistence.impl.PersistenceContextInitializer.class,
      includeClasses = {
            HyperLogLog.class,
            ExplicitSet.class,
            CompactSet.class,
      },
      schemaFileName = "persistence.resp.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.persistence.resp",
      service = false
)
public interface PersistenceContextInitializer extends SerializationContextInitializer { }
