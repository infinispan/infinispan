package org.infinispan.client.hotrod.query.key.tutorial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.query.key.tutorial.QueryByKeyTest")
public class QueryByKeyTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder indexed = new ConfigurationBuilder();
      indexed.statistics().enable();
      indexed.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("tutorial.Person");
      return TestCacheManagerFactory.createServerModeCacheManager(indexed);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TutorialSchema.TUTORIAL_SCHEMA;
   }

   @BeforeMethod
   public void setUp() {
      RemoteCache<PersonKey, Person> remoteCache = remoteCacheManager.getCache();
      if (!remoteCache.isEmpty()) {
         return;
      }
      for (int item = 1; item <= 10; item++) {
         Person person = new Person("name" + item, "last" + item, 2000 + item, "city" + item, item);
         PersonKey personKey = new PersonKey("id" + item, "pseudo" + item, new Author("id" + item, "name" + item));
         remoteCache.put(personKey, person);
      }
   }

   @Test
   public void test() {
      RemoteCache<PersonKey, Person> remoteCache = remoteCacheManager.getCache();

      Query<Person> query = remoteCache.query("FROM tutorial.Person p where p.bornYear = 2007 and p.mykey.id='id7'");
      QueryResult<Person> result = query.execute();
      assertThat( result.count().value() ).isOne();
      assertThat( result.list() ).extracting("firstName").containsExactly("name7");

      query = remoteCache.query("FROM tutorial.Person p where p.bornYear = 2003 and p.mykey.author.id='id3'");
      result = query.execute();
      assertThat( result.count().value() ).isOne();
      assertThat( result.list() ).extracting("firstName").containsExactly("name3");
   }
}
