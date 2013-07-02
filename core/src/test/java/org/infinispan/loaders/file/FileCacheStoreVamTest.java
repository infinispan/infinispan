package org.infinispan.loaders.file;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;

/**
 * FileCacheStoreTest using production level marshaller.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "unit", testName = "loaders.file.FileCacheStoreVamTest")
public class FileCacheStoreVamTest extends FileCacheStoreTest {
   private EmbeddedCacheManager cm;

   @Override
   protected StreamingMarshaller getMarshaller() {
      if (cm == null)
         cm = TestCacheManagerFactory.createLocalCacheManager(false);

      return extractCacheMarshaller(cm.getCache());
   }

   @AfterTest
   public void destroy() {
      if (cm != null) cm.stop();
   }

}
