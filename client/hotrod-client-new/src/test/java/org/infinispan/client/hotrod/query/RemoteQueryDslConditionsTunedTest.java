package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.FILESYSTEM;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.configuration.cache.ConfigurationBuilder;
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
            .storage(FILESYSTEM).path(indexDirectory)
            .addIndexedEntity("sample_bank_account.User")
            .addIndexedEntity("sample_bank_account.Account")
            .addIndexedEntity("sample_bank_account.Transaction")
            .writer().ramBufferSize(220)
            .merge().factor(30).maxSize(4096);

      return builder;
   }

   @Override
   public void testIndexPresence() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);

      // we have indexing for remote query!
      assertNotNull(searchMapping.indexedEntity("sample_bank_account.User"));
      assertNotNull(searchMapping.indexedEntity("sample_bank_account.Account"));
      assertNotNull(searchMapping.indexedEntity("sample_bank_account.Transaction"));

      // we have some indexes for this cache
      assertEquals(3, searchMapping.allIndexedEntities().size());
   }
}
