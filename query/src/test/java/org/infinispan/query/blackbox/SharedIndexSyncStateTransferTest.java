package org.infinispan.query.blackbox;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.testng.annotations.Test;

/**
 * Tests for correctness of shared indexes syncing during state transfer
 */
@Test(groups = "functional", testName = "query.blackbox.SharedIndexSyncStateTransferTest")
public class SharedIndexSyncStateTransferTest extends LocalIndexSyncStateTransferTest {

   @Override
   protected ConfigurationBuilder getBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.indexing().enable()
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addProperty("default.indexmanager", InfinispanIndexManager.class.getName());
      return builder;
   }

   @Override
   protected Map<Class<?>, AtomicInteger> getEntityCountPerClass(Cache<Integer, Object> c) {
      Map<Class<?>, AtomicInteger> countPerEntity = new HashMap<>();
      c.forEach((key, value) -> {
         Class<?> entity = value.getClass();
         countPerEntity.computeIfAbsent(entity, aClass -> new AtomicInteger(0)).incrementAndGet();
      });
      return countPerEntity;
   }
}
