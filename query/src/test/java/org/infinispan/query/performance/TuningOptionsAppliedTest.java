package org.infinispan.query.performance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.IndexAccessor;
import org.infinispan.query.test.Person;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Verifies the options used for performance tuning are actually being applied to the Search engine
 *
 * @author Sanne Grinovero
 * @since 5.3
 */
@Test(groups = "functional", testName = "query.performance.TuningOptionsAppliedTest")
public class TuningOptionsAppliedTest extends AbstractInfinispanTest {

   public void verifyFSDirectoryOptions() throws IOException {
      EmbeddedCacheManager embeddedCacheManager = TestCacheManagerFactory.fromXml("nrt-performance-writer.xml");
      try {
         IndexAccessor indexAccessor = IndexAccessor.of(embeddedCacheManager.getCache("Indexed"), Person.class);
         verifyShardingOptions(indexAccessor, 6);
         verifyUsesFSDirectory(indexAccessor);
      } finally {
         TestingUtil.killCacheManagers(embeddedCacheManager);
      }
   }

   private void verifyShardingOptions(IndexAccessor accessorForTests, int expectedShards) {
      assertThat(accessorForTests.getShardsForTests()).hasSize(expectedShards);
   }

   private void verifyUsesFSDirectory(IndexAccessor accessorForTests) {
      Directory directory = accessorForTests.getDirectory();
      Assert.assertTrue(directory instanceof FSDirectory);
   }
}
