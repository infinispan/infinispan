package org.infinispan.query.remote.indexing;

import org.apache.lucene.search.Query;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.remote.SerializationContextHolder;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.remote.protostream.ProtobufWrapperIndexingTest")
public class ProtobufWrapperIndexingTest extends SingleCacheManagerTest {

   private SerializationContext serCtx;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      serCtx = SerializationContextHolder.getSerializationContext();
      MarshallerRegistration.registerMarshallers(serCtx);

      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing()
            .enable()
            .indexLocalOnly(false)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testIndexingWithWrapper() throws Exception {
      // Store some test data:
      ProtobufValueWrapper wrapper1 = new ProtobufValueWrapper(createMarshalledUser("Adrian", "Nistor"));
      ProtobufValueWrapper wrapper2 = new ProtobufValueWrapper(createMarshalledUser("John", "Batman"));

      cache.put(1, wrapper1);   //todo how do we index if the key is a byte array?
      cache.put(2, wrapper2);

      SearchManager qf = Search.getSearchManager(cache);

      SearchFactoryImplementor searchFactory = (SearchFactoryImplementor) qf.getSearchFactory();
      assertNotNull(searchFactory.getIndexManagerHolder().getIndexManager(ProtobufValueWrapper.class.getName()));

      Query luceneQuery = qf.buildQueryBuilderForClass(ProtobufValueWrapper.class)
            .get()
            .keyword()
            .onField("name")
            .ignoreFieldBridge()   //todo [anistor] ignoring the field bridge is a shameless hack!
            .matching("Adrian")
            .createQuery();

      List<Object> list = qf.getQuery(luceneQuery).list();
      assertEquals(1, list.size());

      // the alternative ....

      QueryBuilder guestQueryBuilder = searchFactory.buildQueryBuilder().forEntity(ProtobufValueWrapper.class).get();
      Query queryAllGuests = guestQueryBuilder
            .keyword()
            .onField("name")
            .ignoreFieldBridge()   //todo [anistor] ignoring the field bridge is a shameless hack!
            .matching("Adrian")
            .createQuery();

      List<EntityInfo> queryEntityInfos = searchFactory.createHSQuery().luceneQuery(queryAllGuests)
            .targetedEntities(Arrays.asList(new Class<?>[]{ProtobufValueWrapper.class}))
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
      user.setAccountIds(Collections.singletonList(12));

      Address address = new Address();
      address.setStreet("Dark Alley");
      address.setPostCode("1234");
      user.setAddresses(Collections.singletonList(address));

      return ProtobufUtil.toWrappedByteArray(serCtx, user);
   }
}
