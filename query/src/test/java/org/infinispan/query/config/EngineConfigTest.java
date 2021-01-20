package org.infinispan.query.config;

import static org.infinispan.query.impl.config.SearchPropertyExtractor.extractProperties;
import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @since 12.0
 */
@Test(groups = "unit", testName = "query.config.EngineConfigTest")
public class EngineConfigTest extends AbstractInfinispanTest {
   EmbeddedCacheManager cacheManager;
   File tempDir;

   @BeforeMethod
   public void createTempDir() throws IOException {
      tempDir = Files.createTempDirectory(EngineConfigTest.class.getName()).toFile();
   }

   @AfterMethod
   public void tearDown() throws Exception {
      TestingUtil.killCacheManagers(cacheManager);
      Util.recursiveFileRemove(tempDir);
   }

   @Test
   public void testPropertiesGeneration() {
      GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder();

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().addIndexedEntity(Person.class)
            .storage(IndexStorage.FILESYSTEM).path(Paths.get(tempDir.getPath(), "baseDir").toString())
            .reader().refreshInterval(5000)
            .writer().commitInterval(2000)
            .ramBufferSize(40)
            .queueSize(555).queueCount(8)
            .threadPoolSize(11).setLowLevelTrace(true).maxBufferedEntries(50000)
            .merge().maxSize(1500).factor(30).calibrateByDeletes(true).minSize(100).maxForcedSize(110).maxEntries(12000);

      Map<String, Object> properties = resolveIndexingProperties(globalConfigurationBuilder, builder);

      // Storage
      assertEquals("local-filesystem", properties.get("hibernate.search.backend.directory.type"));
      assertEquals(tempDir.toString() + "/baseDir", properties.get("hibernate.search.backend.directory.root"));

      // reader
      assertEquals(5000L, properties.get("hibernate.search.backend.io.refresh_interval"));

      // writer
      assertEquals(2000, properties.get("hibernate.search.backend.io.commit_interval"));
      assertEquals(40, properties.get("hibernate.search.backend.io.writer.ram_buffer_size"));
      assertEquals(555, properties.get("hibernate.search.backend.indexing.queue_size"));
      assertEquals(8, properties.get("hibernate.search.backend.indexing.queue_count"));
      assertEquals(11, properties.get("hibernate.search.backend.thread_pool.size"));
      assertEquals(Boolean.TRUE, properties.get("hibernate.search.backend.io.writer.infostream"));
      assertEquals(50000, properties.get("hibernate.search.backend.io.writer.max_buffered_docs"));

      // merge
      assertEquals(1500, properties.get("hibernate.search.backend.io.merge.max_size"));
      assertEquals(30, properties.get("hibernate.search.backend.io.merge.factor"));
      assertEquals(Boolean.TRUE, properties.get("hibernate.search.backend.io.merge.calibrate_by_deletes"));
      assertEquals(100, properties.get("hibernate.search.backend.io.merge.min_size"));
      assertEquals(110, properties.get("hibernate.search.backend.io.merge.max_forced_size"));
      assertEquals(12000, properties.get("hibernate.search.backend.io.merge.max_docs"));
   }

   @Test
   public void testNoIndexLocationWithGlobalState() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.globalState().enabled(true).persistentLocation(tempDir.getPath());

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().addIndexedEntity(Person.class).storage(IndexStorage.FILESYSTEM).create();

      Map<String, Object> properties = resolveIndexingProperties(gcb, builder);

      assertEquals(tempDir.getPath(), properties.get("hibernate.search.backend.directory.root"));
   }

   @Test
   public void testNoIndexLocationWithoutGlobalState() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().addIndexedEntity(Person.class).enable();

      Map<String, Object> properties = resolveIndexingProperties(new GlobalConfigurationBuilder(), builder);
      assertEquals(System.getProperty("user.dir"), properties.get("hibernate.search.backend.directory.root"));

   }

   @Test
   public void testLegacyIndexPathWithoutGlobalState() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().addIndexedEntity(Person.class).addProperty("default.indexBase", tempDir.getPath());

      Map<String, Object> properties = resolveIndexingProperties(new GlobalConfigurationBuilder(), builder);
      assertEquals(tempDir.getPath(), properties.get("hibernate.search.backend.directory.root"));
   }

   @Test
   public void testRelativeIndexLocation() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.globalState().enabled(true).persistentLocation(tempDir.getPath());

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().addIndexedEntity(Person.class)
            .storage(IndexStorage.FILESYSTEM).path("my-index").create();

      Map<String, Object> properties = resolveIndexingProperties(gcb, builder);

      assertEquals(tempDir.getPath() + "/my-index", properties.get("hibernate.search.backend.directory.root"));
   }

   private Map<String, Object> resolveIndexingProperties(GlobalConfigurationBuilder gcb, ConfigurationBuilder builder) {
      cacheManager = TestCacheManagerFactory.createCacheManager(gcb, builder);
      GlobalConfiguration globalConfiguration = cacheManager.getCacheManagerConfiguration();
      IndexingConfiguration indexingConfiguration = cacheManager.getCache().getCacheConfiguration().indexing();
      return extractProperties(globalConfiguration, indexingConfiguration, this.getClass().getClassLoader());
   }
}
