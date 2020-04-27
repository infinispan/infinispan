package org.infinispan.persistence.jpa;

import java.io.File;
import java.nio.file.Paths;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.persistence.PersistenceCompatibilityTest;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;
import org.testng.annotations.Test;

/**
 * Tests if {@link JpaStore} can migrate data from Infinispan 10.1.x.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "persistence.jpa.JpaStoreCompatibilityTest")
public class JpaStoreCompatibilityTest extends PersistenceCompatibilityTest<KeyValueEntity> {

   private static final String DB_FILE_NAME = "jpa_db.mv.db";
   private static final String DATA_10_1_FOLDER = "10_1_x_jpa_data";

   protected JpaStoreCompatibilityTest() {
      super(JpaKeyValueWrapper.INSTANCE);
   }

   @Override
   protected void amendGlobalConfigurationBuilder(GlobalConfigurationBuilder builder) {
      super.amendGlobalConfigurationBuilder(builder);
      builder.serialization().addContextInitializer(JpaSCI.INSTANCE);
   }

   @Override
   protected void beforeStartCache() throws Exception {
      new File(tmpDirectory).mkdirs();
      copyFile(combinePath(DATA_10_1_FOLDER, DB_FILE_NAME), Paths.get(tmpDirectory), DB_FILE_NAME);
   }

   @Override
   protected String cacheName() {
      return "jps-store-cache";
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      builder.persistence().addStore(JpaStoreConfigurationBuilder.class)
            .entityClass(KeyValueEntity.class)
            .storeMetadata(true)
            .persistenceUnitName("org.infinispan.persistence.jpa.compatibility_test")
            .segmented(false);
   }
}
