package org.infinispan.query.maxresult;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.maxresult.LocalHitCountAccuracyTest")
@TestForIssue(jiraKey = "ISPN-14195")
public class LocalHitCountAccuracyTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("org.infinispan.query.model.Game");
      indexed.query().hitCountAccuracy(10); // lower the default accuracy

      EmbeddedCacheManager manager = TestCacheManagerFactory.createCacheManager();
      manager.defineConfiguration("indexed-games", indexed.build());
      return manager;
   }

   @Test
   public void smokeTest() {
      DistributedHitCountAccuracyTest.executeSmokeTest(cacheManager.getCache("indexed-games"));
   }
}
