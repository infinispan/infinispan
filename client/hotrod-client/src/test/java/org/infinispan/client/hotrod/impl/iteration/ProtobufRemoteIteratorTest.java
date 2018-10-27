package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.ProtobufRemoteIteratorTest")
public class ProtobufRemoteIteratorTest extends MultiHotRodServersTest implements AbstractRemoteIteratorTest {

   private static final int NUM_NODES = 2;
   private static final int CACHE_SIZE = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfgBuilder.encoding().key().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      cfgBuilder.encoding().value().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      createHotRodServers(NUM_NODES, cfgBuilder);
      waitForClusterToForm();

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = client(0).getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader()));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      // Register extra marshallers in the ProtobufMetadataManager. In standalone servers this can be done with a deployment jar.
      for (EmbeddedCacheManager cm : cacheManagers) {
         MarshallerRegistration.registerMarshallers(ProtobufMetadataManagerImpl.getSerializationContext(cm));
      }

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(client(0)));
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      return super.createHotRodClientConfigurationBuilder(serverPort)
            .marshaller(new ProtoStreamMarshaller());
   }

   public void testSimpleIteration() {
      RemoteCache<Integer, AccountPB> cache = clients.get(0).getCache();

      populateCache(CACHE_SIZE, this::newAccountPB, cache);

      List<AccountPB> results = new ArrayList<>();
      cache.retrieveEntries(null, null, CACHE_SIZE).forEachRemaining(e -> results.add((AccountPB) e.getValue()));

      assertEquals(CACHE_SIZE, results.size());
   }

   static final class ToStringFilterConverterFactory implements KeyValueFilterConverterFactory<Integer, AccountPB, String>, Serializable {
      @Override
      public KeyValueFilterConverter<Integer, AccountPB, String> getFilterConverter() {
         return new ToStringFilterConverter();
      }
   }

   static final class ToStringFilterConverter extends AbstractKeyValueFilterConverter<Integer, AccountPB, String> implements Serializable {
      @Override
      public String filterAndConvert(Integer key, AccountPB value, Metadata metadata) {
         return value.toString();
      }
   }

   public void testFilteredIteration() {
      servers.forEach(s -> s.addKeyValueFilterConverterFactory("filterName", new ToStringFilterConverterFactory()));

      RemoteCache<Integer, AccountPB> cache = clients.get(0).getCache();

      populateCache(CACHE_SIZE, this::newAccountPB, cache);

      Set<Integer> segments = rangeAsSet(1, 30);
      Set<Entry<Object, Object>> results = new HashSet<>();
      cache.retrieveEntries("filterName", segments, CACHE_SIZE).forEachRemaining(results::add);
      Set<Object> values = extractValues(results);

      assertForAll(values, s -> s instanceof String);

      Marshaller marshaller = clients.iterator().next().getMarshaller();
      LocalizedCacheTopology cacheTopology = advancedCache(0).getDistributionManager().getCacheTopology();

      assertKeysInSegment(results, segments, marshaller, cacheTopology::getSegment);
   }

   public void testFilteredIterationWithQuery() {
      RemoteCache<Integer, AccountPB> remoteCache = clients.get(0).getCache();
      populateCache(CACHE_SIZE, this::newAccountPB, remoteCache);
      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);

      int lowerId = 5;
      int higherId = 8;
      Query simpleQuery = queryFactory.from(AccountPB.class).having("id").between(lowerId, higherId).build();
      Set<Entry<Object, Object>> entries = extractEntries(remoteCache.retrieveEntriesByQuery(simpleQuery, null, 10));
      Set<Integer> keys = extractKeys(entries);

      assertEquals(4, keys.size());
      assertForAll(keys, key -> key >= lowerId && key <= higherId);
      assertForAll(entries, e -> e.getValue() instanceof AccountPB);

      Query projectionsQuery = queryFactory.from(AccountPB.class).select("id", "description").having("id").between(lowerId, higherId).build();
      Set<Entry<Integer, Object[]>> entriesWithProjection = extractEntries(remoteCache.retrieveEntriesByQuery(projectionsQuery, null, 10));

      assertEquals(4, entriesWithProjection.size());
      assertForAll(entriesWithProjection, entry -> {
         Integer id = entry.getKey();
         Object[] value = entry.getValue();
         return value[0] == id && value[1].equals("description for " + id);
      });
   }
}
