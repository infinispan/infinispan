package org.infinispan.multimap.impl;

import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.impl.DelegatingUserMarshaller;
import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "multimap.MultimapStoreBucketTest")
public class MultimapStoreBucketTest extends AbstractInfinispanTest {

   public void testMultimapWithJavaSerializationMarshaller() throws Exception {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.defaultCacheName("test");
      globalBuilder.serialization().marshaller(new JavaSerializationMarshaller()).allowList().addClass(SuperPerson.class.getName());

      ConfigurationBuilder config = new ConfigurationBuilder();
      config.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(globalBuilder, config);

      MultimapCacheManager<String, Person> multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cm);
      MultimapCache<String, Person> multimapCache = multimapCacheManager.get("test");
      multimapCache.put("k1", new SuperPerson());
      PersistenceMarshallerImpl pm = TestingUtil.extractPersistenceMarshaller(cm);
      DelegatingUserMarshaller userMarshaller = (DelegatingUserMarshaller) pm.getUserMarshaller();
      assertTrue(userMarshaller.getDelegate() instanceof JavaSerializationMarshaller);
      assertTrue(pm.getSerializationContext().canMarshall(Bucket.class));
      assertTrue(multimapCache.containsKey("k1").get(1, TimeUnit.SECONDS));
   }
}
