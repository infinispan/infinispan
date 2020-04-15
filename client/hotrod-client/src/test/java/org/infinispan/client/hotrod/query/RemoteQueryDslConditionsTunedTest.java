package org.infinispan.client.hotrod.query;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Verifying that the tuned query configuration also works for Remote Queries.
 *
 * @author Anna Manukyan
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.RemoteQueryDslConditionsTunedTest", groups = "functional")
public class RemoteQueryDslConditionsTunedTest extends RemoteQueryDslConditionsFilesystemTest {

   private static final int NUM_SHARDS = 6;

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.FILE)
            .addProperty(SearchConfig.DIRECTORY_ROOT, indexDirectory)
            .addProperty(SearchConfig.THREAD_POOL_SIZE, String.valueOf(NUM_SHARDS))
            .addProperty(SearchConfig.QUEUE_COUNT, String.valueOf(NUM_SHARDS))
            .addProperty(SearchConfig.QUEUE_SIZE, "4096")
            .addProperty(SearchConfig.COMMIT_INTERVAL, "10000")
            .addProperty(SearchConfig.SHARDING_STRATEGY, SearchConfig.HASH)
            .addProperty(SearchConfig.NUMBER_OF_SHARDS, String.valueOf(NUM_SHARDS));

      return builder;
   }

   @Override
   public void testIndexPresence() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMappingHolder.class)
            .getSearchMapping();

      // we have indexing for remote query!
      assertTrue(searchMapping.allIndexedTypes().containsValue(ProtobufValueWrapper.class));

      // we have some indexes for this cache
      assertEquals(2, searchMapping.allIndexedTypes().size());
   }
}
