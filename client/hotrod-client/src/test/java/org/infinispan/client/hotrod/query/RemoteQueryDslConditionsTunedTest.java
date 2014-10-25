package org.infinispan.client.hotrod.query;

import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.indexes.impl.IndexManagerHolder;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.Search;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.testng.annotations.Test;

import static org.junit.Assert.assertTrue;

/**
 * Verifying that the tuned query configuration also works for Remote Queries.
 *
 * @author Anna Manukyan
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.RemoteQueryDslConditionsTunedTest", groups = "functional")
public class RemoteQueryDslConditionsTunedTest extends RemoteQueryDslConditionsFilesystemTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.dataContainer()
            .keyEquivalence(ByteArrayEquivalence.INSTANCE)
            .valueEquivalence(ByteArrayEquivalence.INSTANCE)
            .indexing().index(Index.ALL)
            .addProperty("default.indexmanager", "near-real-time")
            .addProperty("default.indexBase", indexDirectory)
            .addProperty("default.exclusive_index_use", "true")
            .addProperty("default.indexwriter.merge_factor", "30")
            .addProperty("default.indexwriter.merge_max_size", "4096")
            .addProperty("default.indexwriter.ram_buffer_size", "220")
            .addProperty("default.locking_strategy", "native")
            .addProperty("default.sharding_strategy.nbr_of_shards", "6");

      return builder;
   }

   @Override
   public void testIndexPresence() {
      SearchFactoryImplementor searchFactory = (SearchFactoryImplementor) Search.getSearchManager(getEmbeddedCache()).getSearchFactory();
      IndexManagerHolder indexManagerHolder = searchFactory.getIndexManagerHolder();

      assertTrue(searchFactory.getIndexedTypes().contains(ProtobufValueWrapper.class));
      for (IndexManager manager : indexManagerHolder.getIndexManagers()) {
         assertTrue(manager.getIndexName().contains(ProtobufValueWrapper.class.getName()));
      }
   }
}
