package org.infinispan.jcache;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      dependsOn = JCacheCommonsTestSCI.class,
      includeClasses = {
            InvokeProcessorTest.Entry.class,
            InvokeProcessorTest.TestEntryProcessor.class,
            InvokeProcessorTest.TestExceptionThrowingEntryProcessor.class,
            JCacheCustomKeyGenerator.CustomGeneratedCacheKey.class
      },
      schemaFileName = "test.jcache.embedded.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.jcache.embedded")
public interface JCacheTestSCI extends SerializationContextInitializer {
   JCacheTestSCI INSTANCE = new JCacheTestSCIImpl();
}
