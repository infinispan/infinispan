package org.infinispan.lucene;

import java.io.IOException;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Verifies the externalizers of the lucene-directory module are registered automatically
 *
 * @author Sanne Grinovero
 * @since 5.0
 */
@Test(groups = "functional", testName = "lucene.ExternalizersEnabledTest")
public class ExternalizersEnabledTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createLocalCacheConfiguration();
      return TestCacheManagerFactory.createCacheManager(configurationBuilder);
   }

   @Test
   public void testChunkCacheKeyExternalizer() throws IOException {
      ChunkCacheKey key = new ChunkCacheKey("myIndex", "filename", 5, 1000, 7);
      verifyExternalizerForType(key, ChunkCacheKey.Externalizer.class);
   }

   @Test
   public void testFileCacheKeyExternalizer() throws IOException {
      FileCacheKey key = new FileCacheKey("myIndex", "fileA.idx", 7);
      verifyExternalizerForType(key, FileCacheKey.Externalizer.class);
   }

   @Test
   public void testFileListCacheKeyExternalizer() throws IOException {
      FileListCacheKey key = new FileListCacheKey("myIndex", 7);
      verifyExternalizerForType(key, FileListCacheKey.Externalizer.class);
   }

   @Test
   public void testFileMetadataExternalizer() throws IOException {
      FileMetadata key = new FileMetadata(23);
      verifyExternalizerForType(key, FileMetadata.Externalizer.class);
   }

   @Test
   public void testFileReadLockKeyExternalizer() throws IOException {
      FileReadLockKey key = new FileReadLockKey("myIndex", "index.lock", 7);
      verifyExternalizerForType(key, FileReadLockKey.Externalizer.class);
   }

   private void verifyExternalizerForType(Object keySampleType, Class expectedExcternalizerClass) throws IOException {
      GlobalMarshaller marshaller = TestingUtil.extractGlobalMarshaller(cacheManager);
      Externalizer<Object> ext = marshaller.findExternalizerFor(keySampleType);

      AssertJUnit.assertEquals("Registered Externalizers should be wrapped by a ForeignAdvancedExternalizer",
            ext.getClass().toString(),
            "class org.infinispan.marshall.core.ExternalExternalizers$ForeignAdvancedExternalizer");

      AssertJUnit.assertEquals("Type of delegate used by the adapter doesn't match expected: " + expectedExcternalizerClass.getClass(),
            "class " + ext.toString(),
            expectedExcternalizerClass.toString());
   }

}
