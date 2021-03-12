package org.infinispan.persistence.sifs;

import static org.infinispan.persistence.sifs.SoftIndexFileStore.PREFIX_10_1;
import static org.infinispan.persistence.sifs.SoftIndexFileStore.PREFIX_11_0;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.persistence.IdentityKeyValueWrapper;
import org.infinispan.persistence.PersistenceCompatibilityTest;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.test.data.Value;
import org.testng.annotations.Test;

/**
 * Tests if {@link SoftIndexFileStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreCompatibilityTest")
public class SoftIndexFileStoreCompatibilityTest extends PersistenceCompatibilityTest<Value> {

   private static final Map<Version, VersionMeta> data = new HashMap<>(2);
   static {
      data.put(Version._10_1, new VersionMeta("10_1_x_sifs_data", PREFIX_10_1));
      data.put(Version._11_0, new VersionMeta("11_0_x_sifs_data", PREFIX_11_0));
   }

   public SoftIndexFileStoreCompatibilityTest() {
      super(IdentityKeyValueWrapper.instance());
   }

   @Override
   protected void amendGlobalConfigurationBuilder(GlobalConfigurationBuilder builder) {
      super.amendGlobalConfigurationBuilder(builder);
      builder.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory());
   }

   @Override
   protected void beforeStartCache(Version version) throws Exception {
      Path dataLocation = getStoreLocation(combinePath(tmpDirectory, "data"), "data");
      Path indexLocation = getStoreLocation(combinePath(tmpDirectory, "index"), "index");

      dataLocation.toFile().mkdirs();
      indexLocation.toFile().mkdirs();

      data.get(version).copyFiles(dataLocation, indexLocation);
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

   static class VersionMeta {
      final String root;
      final String prefix;

      VersionMeta(String root, String prefix) {
         this.root = root;
         this.prefix = prefix;
      }

      void copyFiles(Path dataLocation, Path indexLocation) throws IOException {
         String dataPath = String.format("%s/data/%s", root, prefix + 0);
         copyFile(dataPath, dataLocation, prefix + 0);
         for (int i = 0; i < 3; i++) {
            String indexFile = "index." + i;
            String indexPath = String.format("%s/index/%s", root, indexFile);
            copyFile(indexPath, indexLocation, indexFile);
         }
      }
   }
}
