package org.infinispan.persistence.sifs;

import java.nio.file.Path;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.persistence.PersistenceCompatibilityTest;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests if {@link SoftIndexFileStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreCompatibilityTest")
public class SoftIndexFileStoreCompatibilityTest extends PersistenceCompatibilityTest<String> {

   private static final String DATA_10_1 = "10_1_x_sifs_data/data/0";
   private static final String[] INDEX_10_1 = new String[]{
         "10_1_x_sifs_data/index/index.0",
         "10_1_x_sifs_data/index/index.1",
         "10_1_x_sifs_data/index/index.2"
   };

   public SoftIndexFileStoreCompatibilityTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   @Override
   protected void amendGlobalConfigurationBuilder(GlobalConfigurationBuilder builder) {
      super.amendGlobalConfigurationBuilder(builder);
      builder.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory());
   }

   @Override
   protected void beforeStartCache() throws Exception {
      Path dataLocation = getStoreLocation(combinePath(tmpDirectory, "data"), "data");
      Path indexLocation = getStoreLocation(combinePath(tmpDirectory, "index"), "index");

      dataLocation.toFile().mkdirs();
      indexLocation.toFile().mkdirs();

      copyFile(DATA_10_1, dataLocation, "0");
      int i = 0;
      for (String indexPath : INDEX_10_1) {
         copyFile(indexPath, indexLocation, String.format("index.%d", i++));
      }
   }

   @Override
   protected String cacheName() {
      return "sifs-store-cache";
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      builder.persistence().addStore(SoftIndexFileStoreConfigurationBuilder.class)
            .segmented(false)
            .dataLocation(combinePath(tmpDirectory, "data"))
            .indexLocation(combinePath(tmpDirectory, "index"));
   }

}
