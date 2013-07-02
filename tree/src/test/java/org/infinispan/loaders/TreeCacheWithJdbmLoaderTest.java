package org.infinispan.loaders;

import org.infinispan.configuration.cache.LegacyStoreConfigurationBuilder;
import org.infinispan.loaders.jdbm.JdbmCacheStore;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Test tree cache storing data into a cache store that requires data to be
 * serializable as per standard Java rules, such as a the JDBM cache store.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "loaders.TreeCacheWithJdbmLoaderTest")
public class TreeCacheWithJdbmLoaderTest extends TreeCacheWithLoaderTest {

   private String tmpDirectory;

   @Override
   protected void addCacheStore(LegacyStoreConfigurationBuilder cb) {
      cb.cacheStore(new JdbmCacheStore())
         .purgeSynchronously(true) // for more accurate unit testing
         .addProperty("location", tmpDirectory);
   }

   @Override
   protected void setup() throws Exception {
      tmpDirectory = TestingUtil.tmpDirectory(this);
      super.setup();
   }

   @Override
   protected void teardown() {
      super.teardown();
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

}
