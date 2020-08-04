package org.infinispan.query.remote.impl.indexing;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Collections;
import java.util.List;

import org.hibernate.search.engine.search.query.SearchQuery;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.infinispan.query.remote.impl.mapping.SerializationContextSearchMapping;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.remote.impl.indexing.ProtobufValueWrapperIndexingTest")
public class ProtobufValueWrapperIndexingTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .memory().encoding().value().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
            .indexing().enable()
            .addIndexedEntity("sample_bank_account.User")
            .addProperty("directory.type", "local-heap");
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.serialization().addContextInitializer(TestDomainSCI.INSTANCE);
      return TestCacheManagerFactory.createCacheManager(globalBuilder, cfg);
   }

   public void testIndexingWithWrapper() throws Exception {
      SerializationContext serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);

      MarshallerRegistration.registerMarshallers(serCtx);

      // Create Search 6 mapping from current SerializationContext:
      SearchMappingHolder mappingHolder = ComponentRegistryUtils.getSearchMappingHolder(cache);
      SerializationContextSearchMapping.acquire(serCtx).buildMapping(mappingHolder,
            cache.getCacheConfiguration().indexing().indexedEntityTypes());
      SearchMapping searchMapping = mappingHolder.getSearchMapping();
      assertNotNull(searchMapping);

      // Store some test data:
      cache.put(new byte[]{1, 2, 3}, createUser("Adrian", "Nistor"));
      cache.put(new byte[]{4, 5, 6}, createUser("John", "Batman"));

      SearchSession session = searchMapping.getMappingSession();
      SearchScope<byte[]> scope = session.scope(byte[].class, "sample_bank_account.User");
      SearchQuery<Object> query = session.search(scope)
            .select(f -> f.field("surname"))
            .where(f -> f.match().field("name").matching("Adrian"))
            .toQuery();

      List<Object> result = query.fetchAllHits();

      assertEquals(1, result.size());
      assertEquals("Nistor", result.get(0));
   }

   private User createUser(String name, String surname) {
      User user = new User();
      user.setId(1);
      user.setName(name);
      user.setSurname(surname);
      user.setGender(User.Gender.MALE);
      user.setAccountIds(Collections.singleton(12));

      Address address = new Address();
      address.setStreet("Dark Alley");
      address.setPostCode("1234");
      user.setAddresses(Collections.singletonList(address));
      return user;
   }
}
