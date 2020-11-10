package org.infinispan.query.config;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @since 12.0
 */
@Test(groups = "unit", testName = "query.config.EngineConfigTest")
public class EngineConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable().addIndexedEntity(Person.class)
            .storage(IndexStorage.FILESYSTEM).path("${java.io.tmpdir}/baseDir")
            .reader().refreshInterval(5000)
            .writer().commitInterval(2000)
            .ramBufferSize(40)
            .queueSize(555).queueCount(8)
            .threadPoolSize(11).setLowLevelTrace(true).maxBufferedDocs(50000)
            .merge().maxSize(1500).factor(30).calibrateByDeletes(true).minSize(100).maxForcedSize(110).maxDocs(12000);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @Test
   public void testPropertiesGeneration() {
      cache.put(1, new Person("name", "blurb", 12));

      TypedProperties properties = cache.getCacheConfiguration().indexing().properties();

      // Storage
      assertEquals("local-filesystem", properties.get("hibernate.search.backend.directory.type"));
      assertEquals(System.getProperty("java.io.tmpdir") + "/baseDir", properties.get("hibernate.search.backend.directory.root"));

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

}
