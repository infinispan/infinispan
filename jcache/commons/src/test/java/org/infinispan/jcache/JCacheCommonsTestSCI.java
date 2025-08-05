package org.infinispan.jcache;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      includeClasses = AbstractTwoCachesBasicOpsTest.CustomEntryProcessor.class,
      schemaFileName = "test.jcache.commons.proto",
      schemaFilePath = "org/infinispan/jcache",
      schemaPackageName = "org.infinispan.test.jcache.commons",
      service = false
)
public interface JCacheCommonsTestSCI extends SerializationContextInitializer {
   JCacheCommonsTestSCI INSTANCE = new JCacheCommonsTestSCIImpl();
}
