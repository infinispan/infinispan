package org.infinispan.marshall;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.infinispan.commons.marshall.ImmutableProtoStreamMarshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.marshall.core.impl.DelegatingUserMarshaller;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
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
      globalBuilder.serialization().addContextInitializers(new UserSCIImpl(), new AnotherUserSCIImpl());
      createCluster(globalBuilder, new ConfigurationBuilder(), 2);
   }

   @Test(expectedExceptions = MarshallingException.class,
         expectedExceptionsMessageRegExp = "No marshaller registered for object of Java type org\\.infinispan\\.marshall\\.ProtostreamUserMarshallerTest\\$NonMarshallablePojo : .*")
   public void testMarshallingException() throws Exception {
      PersistenceMarshaller pm = TestingUtil.extractPersistenceMarshaller(manager(0));
      pm.objectToBuffer(new NonMarshallablePojo());
   }

   public void testPrimitivesAreMarshallable() throws Exception {
      List<Object> objectsToTest = new ArrayList<>();
      objectsToTest.add("String");
      objectsToTest.add(Integer.MAX_VALUE);
      objectsToTest.add(Long.MAX_VALUE);
      objectsToTest.add(Double.MAX_VALUE);
      objectsToTest.add(Float.MAX_VALUE);
      objectsToTest.add(true);
      objectsToTest.add(new byte[0]);
      objectsToTest.add(Byte.MAX_VALUE);
      objectsToTest.add(Short.MAX_VALUE);
      objectsToTest.add('c');
      objectsToTest.add(new Date());
      objectsToTest.add(Instant.now());

      PersistenceMarshaller pm = TestingUtil.extractPersistenceMarshaller(manager(0));
      for (Object o : objectsToTest) {
         assertTrue(pm.isMarshallable(o));
         assertNotNull(pm.objectToBuffer(o));
      }
   }

   public void testProtostreamMarshallerLoaded() {
      PersistenceMarshaller pm = TestingUtil.extractPersistenceMarshaller(manager(0));
      testIsMarshallableAndPut(pm, new ExampleUserPojo("A Pojo!"), new AnotherExampleUserPojo("And another one!"));
      DelegatingUserMarshaller userMarshaller = (DelegatingUserMarshaller) pm.getUserMarshaller();
      assertTrue(userMarshaller.getDelegate() instanceof ImmutableProtoStreamMarshaller);
   }

   private void testIsMarshallableAndPut(PersistenceMarshaller pm, Object... pojos) {
      for (Object o : pojos) {
         assertTrue(pm.isMarshallable(o));
         String key = o.getClass().getSimpleName();
         cache(0).put(key, o);
         assertNotNull(cache(0).get(key));
      }
   }

   static class ExampleUserPojo {

      @ProtoField(1)
      final String someString;

      @ProtoFactory
      ExampleUserPojo(String someString) {
         this.someString = someString;
      }
   }

   static class AnotherExampleUserPojo {

      @ProtoField(1)
      final String anotherString;

      @ProtoFactory
      AnotherExampleUserPojo(String anotherString) {
         this.anotherString = anotherString;
      }
   }

   static class NonMarshallablePojo {
      int x;
   }

   @AutoProtoSchemaBuilder(
         includeClasses = ExampleUserPojo.class,
         schemaFileName = "test.core.protostream-user-marshall.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.marshall",
         service = false
   )
   interface UserSCI extends SerializationContextInitializer {
   }

   @AutoProtoSchemaBuilder(
         includeClasses = AnotherExampleUserPojo.class,
         schemaFileName = "test.core.protostream-another-user-marshall.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.marshall",
         service = false
   )
   interface AnotherUserSCI extends SerializationContextInitializer {
   }
}
