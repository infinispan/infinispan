package org.infinispan.config.parsing;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "config.parsing.Coherence2InfinispanTransformerTest")
public class Coherence2InfinispanTransformerTest extends AbstractInfinispanTest {

   public static final String XSLT_FILE = "xslt/coherence35x2infinispan4x.xslt";
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
      DefaultCacheManager dcm = null;
      Cache<Object, Object> sampleDistributedCache2 = null;
      try {
         ClassLoader delegatingCl = new Jbc2InfinispanTransformerTest.TestClassLoader(existingCl);
         Thread.currentThread().setContextClassLoader(delegatingCl);
         String fileName = getFileName(coherenceFileName);
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         convertor.parse(fileName, baos, XSLT_FILE);


         File out = new File("target","zzzz2.xml");
         if (out.exists()) out.delete();
         out.createNewFile();
         FileOutputStream fos = new FileOutputStream(out);
         fos.write(baos.toByteArray());
         baos.close();
         fos.close();

         dcm = new DefaultCacheManager(new ByteArrayInputStream(baos.toByteArray()));
         Cache<Object,Object> defaultCache = dcm.getCache();
         defaultCache.put("key", "value");

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
