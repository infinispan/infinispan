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
import org.infinispan.protostream.SerializationContext;
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
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testIndexingWithWrapper() throws Exception {
      SerializationContext serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cacheManager);

      MarshallerRegistration.registerMarshallers(serCtx);

      // Store some test data:
      byte[] value1 = createMarshalledUser(serCtx, "Adrian", "Nistor");
      byte[] value2 = createMarshalledUser(serCtx, "John", "Batman");

      cache.put(new byte[]{1, 2, 3}, value1);
      cache.put(new byte[]{4, 5, 6}, value2);

      SearchManager sm = Search.getSearchManager(cache);

      SearchIntegrator searchFactory = sm.unwrap(SearchIntegrator.class);
      assertNotNull(searchFactory.getIndexManager(ProgrammaticSearchMappingProviderImpl.getIndexName(cache.getName())));

      Query luceneQuery = sm.buildQueryBuilderForClass(ProtobufValueWrapper.class)
            .get()
            .keyword()
            .onField("name")
            .ignoreFieldBridge()
            .ignoreAnalyzer()
            .matching("Adrian")
            .createQuery();

      List<byte[]> list = sm.<byte[]>getQuery(luceneQuery).list();
      assertEquals(1, list.size());
      byte[] pvw = list.get(0);
      User unwrapped = ProtobufUtil.fromWrappedByteArray(serCtx, pvw);
      assertEquals("Adrian", unwrapped.getName());

      // an alternative approach ...

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

   private byte[] createMarshalledUser(SerializationContext serCtx, String name, String surname) throws IOException {
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

      return ProtobufUtil.toWrappedByteArray(serCtx, user);
   }
}
