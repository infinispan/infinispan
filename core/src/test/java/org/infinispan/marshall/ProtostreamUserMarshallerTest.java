package org.infinispan.marshall;

import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * A test to ensure that a {@link org.infinispan.commons.marshall.ProtoStreamMarshaller} instance is loaded as the
 * user marshaller, when a {@link org.infinispan.protostream.SerializationContextInitializer} is configured.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@Test(groups = "functional", testName = "marshall.ProtostreamUserMarshallerTest")
public class ProtostreamUserMarshallerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.serialization().contextInitializer(new UserSCIImpl());
      createCluster(globalBuilder, new ConfigurationBuilder(), 2);
   }

   public void testProtostreamMarshallerLoaded() {
      PersistenceMarshallerImpl pm = (PersistenceMarshallerImpl) TestingUtil.extractPersistenceMarshaller(manager(0));
      assertTrue(pm.isMarshallable(new ExampleUserPojo("Blah")));
      cache(0).put(1, new ExampleUserPojo("Blah"));
   }

   static class ExampleUserPojo {

      @ProtoField(number = 1)
      final String someString;

      @ProtoFactory
      ExampleUserPojo(String someString) {
         this.someString = someString;
      }
   }

   @AutoProtoSchemaBuilder(
         includeClasses = ExampleUserPojo.class,
         schemaFileName = "test.core.protostream-user-marshall.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.marshall")
   interface UserSCI extends SerializationContextInitializer {
   }
}
