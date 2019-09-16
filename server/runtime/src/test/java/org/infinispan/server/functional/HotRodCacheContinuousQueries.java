package org.infinispan.server.functional;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.api.continuous.ContinuousQueryListener;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Katia Aresti &lt;karesti@redhat.com&gt;
 * @since 10.0
 **/
public class HotRodCacheContinuousQueries {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testContinuousQuery() throws IOException {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.marshaller(new ProtoStreamMarshaller());
      RemoteCache<String, Person> cache = SERVER_TEST.getHotRodCache(config, CacheMode.DIST_SYNC);
      RemoteCacheManager rcm = cache.getRemoteCacheManager();

      SerializationContext serializationContext = MarshallerUtil.getSerializationContext(rcm);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile = protoSchemaBuilder.fileName("test.proto").addClass(Person.class).build(serializationContext);

      RemoteCache<String, String> metadataCache = rcm
            .getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("test.proto", protoFile);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));
      cache.clear();
      QueryFactory qf = Search.getQueryFactory(cache);
      Query query = qf.from(Person.class).having("name").eq("Adrian").build();

      ContinuousQuery<String, Person> continuousQuery = Search.getContinuousQuery(cache);

      BlockingQueue<String> people = new LinkedBlockingQueue<>();
      ContinuousQueryListener<String, Person> listener = new ContinuousQueryListener<String, Person>() {
         @Override
         public void resultJoining(String key, Person value) {
            people.add(value.name);
         }
      };
      continuousQuery.addContinuousQueryListener(query, listener);
      cache.put("Adrian", new Person("Adrian", 1));
      assertTrue(cache.containsKey("Adrian"));
      expectElementsInQueue(people, 1);
   }

   private void assertTrue(boolean adrian) {
   }

   private <T, R> void expectElementsInQueue(BlockingQueue<T> queue, int numElements) {
      for (int i = 0; i < numElements; i++) {
         final T o;
         try {
            o = queue.poll(5, TimeUnit.SECONDS);
            assertNotNull("Queue was empty after reading " + i + " elements!", o);
         } catch (InterruptedException e) {
            throw new AssertionError("Interrupted while waiting for condition", e);
         }
      }

      try {
         // no more elements expected here
         Object o = queue.poll(5, TimeUnit.SECONDS);
         assertNull("No more elements expected in queue!", o);
      } catch (InterruptedException e) {
         throw new AssertionError("Interrupted while waiting for condition", e);
      }
   }

   public static class Person {

      @ProtoField(number = 1)
      public String name;

      @ProtoField(number = 2)
      public Integer id;

      @ProtoFactory
      public Person(String name, Integer id) {
         this.name = name;
         this.id = id;
      }
   }
}
