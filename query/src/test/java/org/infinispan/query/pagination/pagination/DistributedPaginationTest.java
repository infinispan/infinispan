package org.infinispan.query.pagination.pagination;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.model.Developer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.testing.annotation.TestForIssue;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.pagination.DistributedPaginationTest")
@TestForIssue(jiraKey = "ISPN-16585")
public class DistributedPaginationTest extends MultipleCacheManagersTest {

   private Cache<Object, Object> node1;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      config
            .clustering().hash().numOwners(2)
            .indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Developer.class);

      createClusteredCaches(2, config);
      node1 = cache(0);
      cache(1);
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void createBeforeMethod() {
      PaginationTest.before(node1);
   }

   @Test
   public void hybrid() {
      PaginationTest.hybrids(node1);
      PaginationTest.hybrids_max3(node1);
   }

   @Test
   public void entityProjection() {
      PaginationTest.entityProjection(node1);
      PaginationTest.entityProjection_max3(node1);
   }

   @Test
   public void defaultProjection() {
      PaginationTest.defaultProjection(node1);
      PaginationTest.defaultProjection_max3(node1);
   }
}
