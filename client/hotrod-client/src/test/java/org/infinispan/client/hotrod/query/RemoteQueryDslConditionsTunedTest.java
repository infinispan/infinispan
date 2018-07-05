package org.infinispan.client.hotrod.query;

import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.Search;
import org.infinispan.query.remote.impl.ProgrammaticSearchMappingProviderImpl;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
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
      builder.indexing().index(Index.ALL)
            .addProperty("default.indexmanager", "near-real-time")
            .addProperty("default.indexBase", indexDirectory)
            .addProperty("default.exclusive_index_use", "true")
            .addProperty("default.indexwriter.merge_factor", "30")
            .addProperty("default.indexwriter.merge_max_size", "4096")
            .addProperty("default.indexwriter.ram_buffer_size", "220")
            .addProperty("default.locking_strategy", "native")
            .addProperty("default.sharding_strategy.nbr_of_shards", String.valueOf(NUM_SHARDS));

      return builder;
   }

   @Override
   public void testIndexPresence() {
      SearchIntegrator searchIntegrator = Search.getSearchManager(getEmbeddedCache()).unwrap(SearchIntegrator.class);
      assertTrue(searchIntegrator.getIndexBindings().containsKey(ProtobufValueWrapper.INDEXING_TYPE));
      for (int shard = 0; shard < NUM_SHARDS; shard++) {
         assertNotNull(searchIntegrator.getIndexManager(ProgrammaticSearchMappingProviderImpl.getIndexName(cache.getName()) + '.' + shard));
      }
   }
}
