package org.infinispan.lucene;

import static org.infinispan.lucene.CacheTestSupport.assertTextIsFoundInIds;
import static org.infinispan.lucene.CacheTestSupport.removeByTerm;
import static org.infinispan.lucene.CacheTestSupport.writeTextToIndex;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * SimpleLuceneTest tests the basic functionality of the Lucene Directory
 * on Infinispan: what is inserted in one node should be able to be found in
 * a second node.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "functional", testName = "lucene.SimpleLuceneTest")
public class SimpleLuceneTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      startClusterNode();
      startClusterNode();
   }

   private void startClusterNode() {
      ConfigurationBuilder configurationBuilder =
            CacheTestSupport.createTestConfiguration(TransactionMode.NON_TRANSACTIONAL);
      configurationBuilder.customInterceptors().addInterceptor().after(NonTransactionalLockingInterceptor.class).interceptor(new SkipIndexingGuaranteed());
      createClusteredCaches(1, "lucene", configurationBuilder);
   }

   @Test
   public void testIndexWritingAndFinding() throws IOException {
      final String indexName = "indexName";
      final Cache<?,?> cache0 = cache(0, "lucene");
      final Cache<?,?> cache1 = cache(1, "lucene");
      Directory dirA = DirectoryBuilder.newDirectoryInstance(cache0, cache0, cache0, indexName).create();
      Directory dirB = DirectoryBuilder.newDirectoryInstance(cache1, cache1, cache1, indexName).create();
      writeTextToIndex(dirA, 0, "hi from node A");
      assertTextIsFoundInIds(dirA, "hi", 0);
      assertTextIsFoundInIds(dirB, "hi", 0);
      writeTextToIndex(dirB, 1, "hello node A, how are you?");
      assertTextIsFoundInIds(dirA, "hello", 1);
      assertTextIsFoundInIds(dirB, "hello", 1);
      assertTextIsFoundInIds(dirA, "node", 1, 0); // node is keyword in both documents id=0 and id=1
      assertTextIsFoundInIds(dirB, "node", 1, 0);
      removeByTerm(dirA, "from");
      assertTextIsFoundInIds(dirB, "node", 1);
      dirA.close();
      dirB.close();
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache0, "indexName");
      DirectoryIntegrityCheck.verifyDirectoryStructure(cache1, "indexName");
   }

   @Test(description="Verifies the caches can be reused after a Directory close")
   public void testCacheReuse() throws IOException {
      testIndexWritingAndFinding();
      cache(0, "lucene").getAdvancedCache().withFlags(Flag.SKIP_INDEXING).clear();
      testIndexWritingAndFinding();
   }

}
