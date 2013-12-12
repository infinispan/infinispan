package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Verifying the functionality of Remote Queries for filesystem directory provider.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.query.RemoteQueryDslConditionsFilestoreTest", groups = "functional")
@CleanupAfterMethod
public class RemoteQueryDslConditionsFilestoreTest extends RemoteQueryDslConditionsTest {

   private final String indexDirectory = TestingUtil.tmpDirectory(getClass());

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.indexing().enable()
            .addProperty("default.directory_provider", getLuceneDirectoryProvider())
            .addProperty("default.indexBase", indexDirectory)
            .addProperty("lucene_version", "LUCENE_CURRENT");

      return builder;
   }

   @Override
   public String getLuceneDirectoryProvider() {
      return "filesystem";
   }

   @Override
   protected void setup() throws Exception {
      TestingUtil.recursiveFileRemove(indexDirectory);
      boolean created = new File(indexDirectory).mkdirs();
      assertTrue(created);
      super.setup();
   }

   @Override
   protected void teardown() {
      try {
         //first stop cache managers, then clear the index
         super.teardown();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         TestingUtil.recursiveFileRemove(indexDirectory);
      }
   }
}
