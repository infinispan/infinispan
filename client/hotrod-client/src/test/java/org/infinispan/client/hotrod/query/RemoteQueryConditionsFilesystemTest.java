package org.infinispan.client.hotrod.query;

import static org.testng.AssertJUnit.assertTrue;

import java.io.File;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.query.RemoteQueryConditionsFilesystemTest", groups = "functional")
public class RemoteQueryConditionsFilesystemTest extends RemoteQueryConditionsTest {

   protected final String indexDirectory = Testing.tmpDirectory(getClass());

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
      builder.indexing().storage(IndexStorage.FILESYSTEM).path(indexDirectory);
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
