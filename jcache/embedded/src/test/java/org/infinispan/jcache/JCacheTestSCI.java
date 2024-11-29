package org.infinispan.jcache;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      dependsOn = JCacheCommonsTestSCI.class,
      includeClasses = {
            InvokeProcessorTest.Entry.class,
            InvokeProcessorTest.TestEntryProcessor.class,
            InvokeProcessorTest.TestExceptionThrowingEntryProcessor.class,
      },
      schemaFileName = "test.jcache.embedded.proto",
      schemaFilePath = "proto/generated",
      schemaPackageName = "org.infinispan.test.jcache.embedded",
      service = false
)
public interface JCacheTestSCI extends SerializationContextInitializer {
   JCacheTestSCI INSTANCE = new JCacheTestSCIImpl();
}
