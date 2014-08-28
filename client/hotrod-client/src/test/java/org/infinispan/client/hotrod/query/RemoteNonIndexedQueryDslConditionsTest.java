package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;

/**
 * Test for query conditions (filtering) without an index. Exercises the whole query DSL on the sample domain model.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteNonIndexedQueryDslConditionsTest")
public class RemoteNonIndexedQueryDslConditionsTest extends RemoteQueryDslConditionsTest {

   protected ConfigurationBuilder getConfigurationBuilder() {
      return hotRodCacheConfiguration();
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Indexing was not enabled on this cache.*")
   @Override
   public void testIndexPresence() {
      org.infinispan.query.Search.getSearchManager(getEmbeddedCache()).getSearchFactory();
   }

   @Test
   @Override
   public void testEqNonIndexed() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("notes").eq("Lorem ipsum dolor sit amet")
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN000405:.*")
   @Override
   public void testInvalidEmbeddedAttributeQuery() throws Exception {
      super.testInvalidEmbeddedAttributeQuery();
   }

   @Test
   @Override
   public void testNullOnIntegerField() throws Exception {
      super.testNullOnIntegerField();
   }
}
