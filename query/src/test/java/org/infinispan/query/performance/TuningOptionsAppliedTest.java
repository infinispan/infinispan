package org.infinispan.query.performance;

import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.hibernate.search.backend.configuration.impl.IndexWriterSetting;
import org.hibernate.search.backend.spi.LuceneIndexingParameters;
import org.hibernate.search.backend.spi.LuceneIndexingParameters.ParameterSet;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.indexes.impl.IndexManagerHolder;
import org.hibernate.search.indexes.impl.NRTIndexManager;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.infinispan.impl.InfinispanDirectoryProvider;
import org.hibernate.search.store.DirectoryProvider;
import org.hibernate.search.store.impl.FSDirectoryProvider;
import org.infinispan.Cache;
import org.infinispan.lucene.impl.DirectoryExtensions;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.Person;
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
public class TuningOptionsAppliedTest {

   public void verifyFSDirectoryOptions() throws IOException {
      EmbeddedCacheManager embeddedCacheManager = TestCacheManagerFactory.fromXml("nrt-performance-writer.xml");
      try {
         SearchFactoryImplementor si = extractSearchFactoryImplementor(embeddedCacheManager);
         NRTIndexManager nrti = verifyShardingOptions(si, 6);
         verifyIndexWriterOptions(nrti, 220, 4096, 30);
         verifyUsesFSDirectory(nrti);
      }
      finally {
         TestingUtil.killCacheManagers(embeddedCacheManager);
      }
   }

   public void verifyInfinispanDirectoryOptions() throws IOException, IllegalArgumentException, IllegalAccessException, SecurityException, NoSuchFieldException {
      EmbeddedCacheManager embeddedCacheManager = TestCacheManagerFactory.fromXml("nrt-performance-writer-infinispandirectory.xml");
      try {
         SearchFactoryImplementor si = extractSearchFactoryImplementor(embeddedCacheManager);
         NRTIndexManager nrti = verifyShardingOptions(si, 6);
         verifyIndexWriterOptions(nrti, 64, 1024, 30);
         verifyUsesInfinispanDirectory(nrti, 128000, embeddedCacheManager);
         //Make sure to close the Indexed cache before those storing the index:
         embeddedCacheManager.getCache("Indexed").stop();
      }
      finally {
         TestingUtil.killCacheManagers(embeddedCacheManager);
      }
   }

   private SearchFactoryImplementor extractSearchFactoryImplementor(EmbeddedCacheManager embeddedCacheManager) {
      Cache<Object, Object> cache = embeddedCacheManager.getCache("Indexed");
      cache.put("hey this type exists", new Person("id", "name", 3));
      SearchManager searchManager = Search.getSearchManager(cache);
      return (SearchFactoryImplementor)searchManager.getSearchFactory();
   }

   private NRTIndexManager verifyShardingOptions(SearchFactoryImplementor searchFactory, int expectedShards) {
      IndexManagerHolder allIndexesManager = searchFactory.getIndexManagerHolder();
      for (int i = 0; i < expectedShards; i++)
         Assert.assertNotNull(allIndexesManager.getIndexManager("person."+i), "person."+i+" IndexManager missing!");
      Assert.assertNull(allIndexesManager.getIndexManager("person."+expectedShards), "An IndexManager too much was created!");

      IndexManager indexManager = allIndexesManager.getIndexManager("person.0");
      Assert.assertTrue(indexManager instanceof NRTIndexManager);
      NRTIndexManager nrtIM = (NRTIndexManager)indexManager;
      return nrtIM;
   }

   private void verifyUsesFSDirectory(NRTIndexManager nrtIM) {
      DirectoryProvider directoryProvider = nrtIM.getDirectoryProvider();
      Assert.assertTrue(directoryProvider instanceof FSDirectoryProvider);
   }

   private void verifyUsesInfinispanDirectory(NRTIndexManager nrti, int expectedChunkSize, EmbeddedCacheManager embeddedCacheManager) throws IllegalArgumentException, IllegalAccessException, SecurityException, NoSuchFieldException {
      DirectoryProvider directoryProvider = nrti.getDirectoryProvider();
      Assert.assertTrue(directoryProvider instanceof InfinispanDirectoryProvider);
      InfinispanDirectoryProvider ispn = (InfinispanDirectoryProvider)directoryProvider;
      Directory infinispanDirectory = ispn.getDirectory();
      DirectoryExtensions extended = (DirectoryExtensions)infinispanDirectory;

      Assert.assertEquals(expectedChunkSize, extended.getChunkSize());

      Assert.assertEquals(extended.getMetadataCache().getName(), "LuceneIndexesMetadataOWR");

      Assert.assertEquals(extended.getDataCache().getName(), "LuceneIndexesDataOWR");

      //And finally check we're running it in the same CacheManager:
      Assert.assertTrue(extended.getDataCache().getCacheManager() == embeddedCacheManager);
   }

   private void verifyIndexWriterOptions(NRTIndexManager nrtIM, Integer expectedRAMBuffer, Integer expectedMaxMergeSize, Integer expectedMergeFactor) {
      LuceneIndexingParameters indexingParameters = nrtIM.getIndexingParameters();
      ParameterSet indexParameters = indexingParameters.getIndexParameters();
      Assert.assertEquals(indexParameters.getCurrentValueFor(IndexWriterSetting.RAM_BUFFER_SIZE), expectedRAMBuffer);
      Assert.assertEquals(indexParameters.getCurrentValueFor(IndexWriterSetting.MERGE_MAX_SIZE), expectedMaxMergeSize);
      Assert.assertEquals(indexParameters.getCurrentValueFor(IndexWriterSetting.MERGE_FACTOR), expectedMergeFactor);
   }

}
