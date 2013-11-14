package org.infinispan.client.hotrod.query;

import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.jgroups.util.Util.assertTrue;

/**
 * Verifying that the tuned query configuration also works for Remote Queries.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.query.HotRodTunedQueryTest", groups = "functional")
@CleanupAfterMethod
public class HotRodTunedQueryTest extends RemoteQueryDslConditionsTest {
   protected String indexBaseDirName = "/remoteIndexDir";

   protected ConfigurationBuilder getConfigurationBuilder() {
      boolean created = new File(System.getProperty(tmpDirPropertyName) + indexBaseDirName).mkdirs();
      assertTrue(created);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.dataContainer()
            .keyEquivalence(ByteArrayEquivalence.INSTANCE)
            .valueEquivalence(ByteArrayEquivalence.INSTANCE)
            .indexing().enable()
            .indexLocalOnly(false)
            .addProperty("default.indexmanager", "near-real-time")
            .addProperty("default.indexBase", System.getProperty(tmpDirPropertyName) + indexBaseDirName)
            .addProperty("default.exclusive_index_use", "true")
            .addProperty("default.indexwriter.merge_factor", "30")
            .addProperty("default.indexwriter.merge_max_size", "4096")
            .addProperty("default.indexwriter.ram_buffer_size", "220")
            .addProperty("default.locking_strategy", "native")
            .addProperty("default.sharding_strategy.nbr_of_shards", "6")
            .addProperty("lucene_version", "LUCENE_36");

      return builder;
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
