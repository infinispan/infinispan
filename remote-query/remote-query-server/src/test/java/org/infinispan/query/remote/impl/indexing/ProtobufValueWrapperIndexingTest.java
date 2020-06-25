package org.infinispan.query.remote.impl.indexing;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.remote.impl.ProgrammaticSearchMappingProviderImpl;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
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
         .addProperty("default.directory_provider", "local-heap")
         .addProperty("lucene_version", "LUCENE_CURRENT");
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.serialization().addContextInitializer(TestDomainSCI.INSTANCE);
      return TestCacheManagerFactory.createCacheManager(globalBuilder, cfg);
   }

   public void testIndexingWithWrapper() throws Exception {
      SerializationContext serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);

      MarshallerRegistration.registerMarshallers(serCtx);

      // Store some test data:
      cache.put(new byte[]{1, 2, 3}, createUser("Adrian", "Nistor"));
      cache.put(new byte[]{4, 5, 6}, createUser("John", "Batman"));

      SearchIntegrator searchFactory = TestingUtil.extractComponent(cache, SearchIntegrator.class);
      assertNotNull(searchFactory.getIndexManager(ProgrammaticSearchMappingProviderImpl.getIndexName(cache.getName())));

      Query luceneQuery2 = searchFactory.buildQueryBuilder().forEntity(ProtobufValueWrapper.class).get()
            .keyword()
            .onField("name")
            .ignoreFieldBridge()
            .ignoreAnalyzer()
            .matching("Adrian")
            .createQuery();

      List<EntityInfo> queryEntityInfos = searchFactory.createHSQuery(luceneQuery2, ProtobufValueWrapper.class)
            .projection("surname")
            .queryEntityInfos();

      assertEquals(1, queryEntityInfos.size());
      EntityInfo entityInfo = queryEntityInfos.get(0);
      assertEquals("Nistor", entityInfo.getProjection()[0]);
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
