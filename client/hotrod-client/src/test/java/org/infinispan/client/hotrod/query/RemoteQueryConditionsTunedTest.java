package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.FILESYSTEM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.sampledomain.bank.Account;
import org.infinispan.protostream.sampledomain.bank.Transaction;
import org.infinispan.protostream.sampledomain.bank.User;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.query.RemoteQueryConditionsTunedTest", groups = "functional")
public class RemoteQueryConditionsTunedTest extends RemoteQueryConditionsFilesystemTest {

   private static final int NUM_SHARDS = 6;

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(FILESYSTEM).path(indexDirectory)
            .addIndexedEntity(User.ENTITY_NAME)
            .addIndexedEntity(Account.ENTITY_NAME)
            .addIndexedEntity(Transaction.ENTITY_NAME)
            .writer().ramBufferSize(220)
            .merge().factor(30).maxSize(4096);

      return builder;
   }

   @Override
   public void testIndexPresence() {
      SearchMapping searchMapping = TestingUtil.extractComponent(cache, SearchMapping.class);

      // we have indexing for remote query!
      assertNotNull(searchMapping.indexedEntity(User.ENTITY_NAME));
      assertNotNull(searchMapping.indexedEntity(Account.ENTITY_NAME));
      assertNotNull(searchMapping.indexedEntity(Transaction.ENTITY_NAME));

      // we have some indexes for this cache
      assertEquals(3, searchMapping.allIndexedEntities().size());
   }
}
