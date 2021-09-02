package org.infinispan.persistence.file;

import static org.infinispan.persistence.file.SingleFileStore.getStoreFile;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.cache.AbstractSegmentedStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.AbstractPersistenceCompatibilityTest;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.test.data.Value;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests if {@link SingleFileStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.file.SingleFileStoreCompatibilityTest")
public class SingleFileStoreCompatibilityTest extends AbstractPersistenceCompatibilityTest<Value> {

   private static final Map<Version, String> files = new HashMap<>();

   static {
      files.put(Version._10_1, "sfs/10_1/sfs-store-cache.dat");
      files.put(Version._11_0, "sfs/11_0/sfs-store-cache.dat");
   }

   private static final Map<Version, byte[]> magic = new HashMap<>();
   static {
      magic.put(Version._10_1, SingleFileStore.MAGIC_BEFORE_11);
      magic.put(Version._11_0, SingleFileStore.MAGIC_11_0);
   }

   public SingleFileStoreCompatibilityTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   @DataProvider
   public Object[][] segmented() {
      return new Object[][] {
            {false, false},
            {false, true},
            {true, false},
            {true, true},
      };
   }

   @Test(dataProvider = "segmented")
   public void testReadWriteFrom101(boolean oldSegmented, boolean newSegmented) throws Exception {
      setParameters(Version._10_1, oldSegmented, newSegmented);
      beforeStartCache();

      cacheManager.defineConfiguration(cacheName(), cacheConfiguration(false).build());
      Exceptions.expectException(CacheConfigurationException.class, CompletionException.class, ".*ISPN000616.*",
                                 () -> cacheManager.getCache(cacheName()));
   }

   @Test(dataProvider = "segmented")
   public void testReadWriteFrom11(boolean oldSegmented, boolean newSegmented) throws Exception {
      setParameters(Version._11_0, oldSegmented, newSegmented);

      doTestReadWrite();
   }

   @Override
   protected void beforeStartCache() throws Exception {
      InputStream is = FileLookupFactory.newInstance().lookupFile(files.get(oldVersion),
                                                                  Thread.currentThread().getContextClassLoader());

      //copy data to the store file
      if (oldSegmented) {
         File segment1File = getSegmentFile(1);
         createParentDirectories(segment1File);
         Files.copy(is, segment1File.toPath(), StandardCopyOption.REPLACE_EXISTING);

         File segment2File = getSegmentFile(2);
         createParentDirectories(segment2File);
         Files.write(segment2File.toPath(), magic.get(oldVersion));
      } else {
         File sfsFile = getStoreFile(tmpDirectory, cacheName());
         createParentDirectories(sfsFile);
         Files.copy(is, sfsFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
   }

   private void createParentDirectories(File file) {
      if (!file.getParentFile().exists()) {
         assertTrue(file.getParentFile().mkdirs());
      }
   }

   private File getSegmentFile(int segment) {
      String segmentPath = AbstractSegmentedStoreConfiguration.fileLocationTransform(tmpDirectory, segment);
      return getStoreFile(segmentPath, cacheName());
   }

   @Override
   protected String cacheName() {
      return "sfs-cache-store";
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder, boolean generatingData) {
      builder.persistence().addSingleFileStore()
            .segmented(oldSegmented)
            .location(tmpDirectory);
   }
}
