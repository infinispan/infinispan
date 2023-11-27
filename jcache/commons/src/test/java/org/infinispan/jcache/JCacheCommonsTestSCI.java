package org.infinispan.jcache;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      includeClasses = AbstractTwoCachesBasicOpsTest.CustomEntryProcessor.class,
      schemaFileName = "test.jcache.commons.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.jcache.commons",
      service = false
)
public interface JCacheCommonsTestSCI extends SerializationContextInitializer {
   JCacheCommonsTestSCI INSTANCE = new JCacheCommonsTestSCIImpl();
}
