package org.infinispan.marshall.persistence.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.proto.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

/**
 * A test to ensure that user configured {@link org.infinispan.protostream.SerializationContextInitializer}
 * implementations are correctly loaded by the {@link org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@Test(groups = "functional", testName = "marshall.UserSerializationContextInitializerTest")
public class UserSerializationContextInitializerTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = UserSerializationContextInitializerTest.class.getSimpleName();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      SerializationContextInitializer sci = new ContextInitializerImpl();
      GlobalConfiguration globalCfg = new GlobalConfigurationBuilder().serialization().contextInitializer(sci)
            .defaultCacheName(CACHE_NAME)
            .build();

      Configuration cfg = getDefaultStandaloneCacheConfig(false).persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .build();
      return new DefaultCacheManager(globalCfg, cfg);
   }

   public void testSerializationContextInitializerLoaded() {
      PersistenceMarshallerImpl pm = (PersistenceMarshallerImpl) TestingUtil.extractPersistenceMarshaller(cacheManager);
      ProtoStreamMarshaller userMarshaller = (ProtoStreamMarshaller) pm.getUserMarshaller();
      SerializationContext ctx = userMarshaller.getSerializationContext();
      assertTrue(ctx.canMarshall(Person.class));
      String key = "k1";
      Person p1 = new Person("Shearer");
      Cache<String, Person> cache = cacheManager.getCache(CACHE_NAME);
      cache.put(key, p1);
      assertEquals(cache.get(key), p1);
   }

   @AutoProtoSchemaBuilder(
         includeClasses = {
               Address.class,
               Person.class
         },
         schemaFileName = "persistence.test.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.persistence.test")
   interface ContextInitializer extends SerializationContextInitializer {
   }
}
