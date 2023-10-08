package org.infinispan.marshall;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.ServiceLoadSerializationContextInitializerTest")
public class ServiceLoadSerializationContextInitializerTest extends AbstractInfinispanTest {

   public void testSCILoaded() throws Exception {
      try (EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(true)) {
         SerializationContextRegistry registry = TestingUtil.extractGlobalComponent(cm, SerializationContextRegistry.class);
         assertTrue(registry.getUserCtx().canMarshall(ServiceLoadedClass.class));
         assertTrue(registry.getGlobalCtx().canMarshall(ServiceLoadedClass.class));
         assertFalse(registry.getPersistenceCtx().canMarshall(ServiceLoadedClass.class));
      }
   }

   static class ServiceLoadedClass {
      @ProtoField(1)
      String field;
   }

   // Service must be true to ensure that the SCI is loaded as expected
   @ProtoSchema(
         includeClasses = ServiceLoadedClass.class,
         schemaFileName = "test.core.protostream-service-loaded-class.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.marshall"
   )
   interface ServiceLoadedSci extends SerializationContextInitializer {
   }
}
