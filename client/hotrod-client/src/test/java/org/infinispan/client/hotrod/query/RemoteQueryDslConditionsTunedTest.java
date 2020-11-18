package org.infinispan.client.hotrod.query;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.search.mapper.mapping.SearchMapping;
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
            .addIndexedEntity("sample_bank_account.User")
            .addIndexedEntity("sample_bank_account.Account")
            .addIndexedEntity("sample_bank_account.Transaction")
            .addProperty(SearchConfig.IO_STRATEGY, SearchConfig.NEAR_REAL_TIME)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.FILE)
            .addProperty(SearchConfig.DIRECTORY_ROOT, indexDirectory)
            .addProperty(SearchConfig.IO_MERGE_FACTOR, "30")
            .addProperty(SearchConfig.IO_MERGE_MAX_SIZE, "4096")
            .addProperty(SearchConfig.IO_WRITER_RAM_BUFFER_SIZE, "220")
            .addProperty(SearchConfig.NUMBER_OF_SHARDS, String.valueOf(NUM_SHARDS))
            .addProperty(SearchConfig.SHARDING_STRATEGY, SearchConfig.HASH);

      return builder;
   }

   @Override
   public void testIndexPresence() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);

      // we have indexing for remote query!
      assertTrue(searchMapping.allIndexedTypes().containsValue(ProtobufValueWrapper.class));

      // we have some indexes for this cache
      assertEquals(2, searchMapping.allIndexedTypes().size());
   }
}
