package org.infinispan.query.remote.impl.indexing;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.remote.impl.ProgrammaticSearchMappingProviderImpl;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.remote.impl.indexing.ProtobufWrapperIndexingTest")
public class ProtobufWrapperIndexingTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testIndexingWithWrapper() throws Exception {
      MarshallerRegistration.registerMarshallers(ProtobufMetadataManagerImpl.getSerializationContext(cacheManager));

      // Store some test data:
      ProtobufValueWrapper wrapper1 = new ProtobufValueWrapper(createMarshalledUser("Adrian", "Nistor"));
      ProtobufValueWrapper wrapper2 = new ProtobufValueWrapper(createMarshalledUser("John", "Batman"));

      cache.put(1, wrapper1);   //todo how do we index if the key is a byte array?
      cache.put(2, wrapper2);

      SearchManager sm = Search.getSearchManager(cache);

      SearchIntegrator searchFactory = sm.unwrap(SearchIntegrator.class);
      assertNotNull(searchFactory.getIndexManager(cache.getName() + ProgrammaticSearchMappingProviderImpl.INDEX_NAME_SUFFIX));

      Query luceneQuery = sm.buildQueryBuilderForClass(ProtobufValueWrapper.class)
            .get()
            .keyword()
            .onField("name")
            .ignoreFieldBridge()
            .ignoreAnalyzer()
            .matching("Adrian")
            .createQuery();

      List<Object> list = sm.getQuery(luceneQuery).list();
      assertEquals(1, list.size());
      ProtobufValueWrapper pvw = (ProtobufValueWrapper) list.get(0);
      User unwrapped = (User) ProtobufUtil.fromWrappedByteArray(ProtobufMetadataManagerImpl.getSerializationContextInternal(cacheManager), pvw.getBinary());
      assertEquals("Adrian", unwrapped.getName());

      // an alternative approach ...

      Query luceneQuery2 = searchFactory.buildQueryBuilder().forEntity(ProtobufValueWrapper.class).get()
            .keyword()
            .onField("name")
            .ignoreFieldBridge()
            .ignoreAnalyzer()
            .matching("Adrian")
            .createQuery();

      List<EntityInfo> queryEntityInfos = searchFactory.createHSQuery().luceneQuery(luceneQuery2)
            .targetedEntities(Collections.singletonList(ProtobufValueWrapper.class))
            .projection("surname")
            .queryEntityInfos();

      assertEquals(1, queryEntityInfos.size());
      EntityInfo entityInfo = queryEntityInfos.get(0);
      assertEquals("Nistor", entityInfo.getProjection()[0]);
   }

   private byte[] createMarshalledUser(String name, String surname) throws IOException {
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

      return ProtobufUtil.toWrappedByteArray(ProtobufMetadataManagerImpl.getSerializationContextInternal(cacheManager), user);
   }
}
