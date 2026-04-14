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
            org.infinispan.server.resp.commands.bloom.BloomFilter.class,
            org.infinispan.server.resp.commands.bloom.BloomFilter.SubFilter.class,
            org.infinispan.server.resp.commands.cuckoo.CuckooFilter.class,
            org.infinispan.server.resp.commands.cuckoo.CuckooFilter.SubFilter.class,
            org.infinispan.server.resp.commands.countmin.CountMinSketch.class,
      },
      schemaFileName = "persistence.resp.proto",
      schemaFilePath = "org/infinispan/server/resp",
      schemaPackageName = "org.infinispan.persistence.resp",
      service = false,
      orderedMarshallers = true
)
public interface PersistenceContextInitializer extends SerializationContextInitializer { }
