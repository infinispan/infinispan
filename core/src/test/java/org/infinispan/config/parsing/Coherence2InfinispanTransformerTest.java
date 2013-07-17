package org.infinispan.config.parsing;

import org.infinispan.Cache;
import org.infinispan.config.parsing.ConfigFilesConvertor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

/**
 * //todo re-enable test as it makes the suite hang
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.parsing.Coherence2InfinispanTransformerTest", enabled = false)
public class Coherence2InfinispanTransformerTest extends AbstractInfinispanTest {

   private static final String BASE_DIR = "configs/coherence";


   ConfigFilesConvertor convertor = new ConfigFilesConvertor();

   public void testDefaultConfigFile() throws Exception {
      testAllFile("/default-config.xml");
   }

   /**
    * Transforms and tests the transformation of a complex file.
    */
   private void testAllFile(String coherenceFileName) throws Exception {
      ClassLoader existingCl = Thread.currentThread().getContextClassLoader();
      CacheContainer dcm = null;
      Cache<Object, Object> sampleDistributedCache2 = null;
      try {
         ClassLoader delegatingCl = new Jbc2InfinispanTransformerTest.TestClassLoader(existingCl);
         Thread.currentThread().setContextClassLoader(delegatingCl);
         String fileName = getFileName(coherenceFileName);
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         convertor.parse(fileName, baos, ConfigFilesConvertor.TRANSFORMATIONS.get(ConfigFilesConvertor.COHERENCE_35X), Thread.currentThread().getContextClassLoader());
         dcm = TestCacheManagerFactory.fromStream(new ByteArrayInputStream(baos.toByteArray()));
         Cache<Object,Object> defaultCache = dcm.getCache();
         defaultCache.put("key", "value");
         Cache<Object, Object> cache = dcm.getCache("dist-*");
         cache.put("a","v");

      } finally {
         Thread.currentThread().setContextClassLoader(existingCl);
         TestingUtil.killCaches(sampleDistributedCache2);
         TestingUtil.killCacheManagers(dcm);
      }
   }

   private String getFileName(String s) {
      return BASE_DIR + File.separator + s;
   }
   

}
