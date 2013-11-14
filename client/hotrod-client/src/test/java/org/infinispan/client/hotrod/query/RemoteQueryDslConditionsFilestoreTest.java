package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.jgroups.util.Util.assertTrue;

/**
 * Verifying the functionality of Remote Queries for filesystem directory provider.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.query.RemoteQueryDslConditionsFilestoreTest", groups = "functional")
@CleanupAfterMethod
public class RemoteQueryDslConditionsFilestoreTest extends RemoteQueryDslConditionsTest {

   protected String indexBaseDirName = "/fileStoreIndexingDir";

   protected ConfigurationBuilder getConfigurationBuilder() {
      boolean created = new File(System.getProperty(tmpDirPropertyName) + indexBaseDirName).mkdirs();
      assertTrue(created);

      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.indexing().enable()
            .addProperty("default.directory_provider", getDirectoryProvider())
            .addProperty("default.indexBase", System.getProperty(tmpDirPropertyName) + indexBaseDirName)
            .addProperty("lucene_version", "LUCENE_CURRENT");

      return builder;
   }

   public String getDirectoryProvider() {
      return "filesystem";
   }

   @Override
   @AfterMethod
   protected void destroyAfterMethod() {
      try {
         //first stop cache managers, then clear the index
         super.destroyAfterMethod();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         TestingUtil.recursiveFileRemove(System.getProperty(tmpDirPropertyName) + indexBaseDirName);
      }
   }

}
