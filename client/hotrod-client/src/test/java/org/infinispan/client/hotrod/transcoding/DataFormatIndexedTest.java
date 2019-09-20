package org.infinispan.client.hotrod.transcoding;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.testng.annotations.Test;

/**
 * Tests for the Hot Rod client using multiple data formats with indexing enabled
 *
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.transcoding.DataFormatIndexedTest")
public class DataFormatIndexedTest extends DataFormatTest {

   @Override
   protected ConfigurationBuilder buildCacheConfig() {
      ConfigurationBuilder parentBuilder = hotRodCacheConfiguration();
      parentBuilder.indexing().index(Index.PRIMARY_OWNER).addProperty("default.directory_provider", "local-heap");
      return parentBuilder;
   }
}
