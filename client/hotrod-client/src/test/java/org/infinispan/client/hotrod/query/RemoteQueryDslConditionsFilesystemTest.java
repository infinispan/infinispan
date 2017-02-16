package org.infinispan.client.hotrod.query;

import static org.testng.AssertJUnit.assertTrue;

import java.io.File;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Verifying the functionality of Remote Queries for filesystem directory provider.
 *
 * @author Anna Manukyan
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.RemoteQueryDslConditionsFilesystemTest", groups = "functional")
public class RemoteQueryDslConditionsFilesystemTest extends RemoteQueryDslConditionsTest {

   protected final String indexDirectory = TestingUtil.tmpDirectory(getClass());

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(indexDirectory);
      boolean created = new File(indexDirectory).mkdirs();
      assertTrue(created);

      super.createCacheManagers();
   }

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = super.getConfigurationBuilder();
      builder.indexing()
            .addProperty("default.directory_provider", "filesystem")
            .addProperty("default.indexBase", indexDirectory);
      return builder;
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      try {
         //first stop cache managers, then clear the index
         super.destroy();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         Util.recursiveFileRemove(indexDirectory);
      }
   }
}
