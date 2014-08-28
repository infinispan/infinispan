package org.infinispan.query.dsl.embedded;

import org.hibernate.hql.ParsingException;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for query conditions (filtering) on cache without indexing. Exercises the whole query DSL on the sample domain
 * model.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.dsl.NonIndexedQueryDslConditionsTest")
public class NonIndexedQueryDslConditionsTest extends QueryDslConditionsTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      createClusteredCaches(1, cfg);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Indexing was not enabled on this cache.*")
   @Override
   public void testIndexPresence() {
      // this is expected to throw an exception
      Search.getSearchManager((Cache) getCacheForQuery()).getSearchFactory();
   }

   @Override
   public void testQueryFactoryType() {
      assertEquals(EmbeddedQueryFactory.class, getQueryFactory().getClass());
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

   @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "ISPN000405:.*")
   @Override
   public void testInvalidEmbeddedAttributeQuery() throws Exception {
      super.testInvalidEmbeddedAttributeQuery();
   }

   /**
    * This test works for non-indexed mode so we re-enable it here.
    */
   @Test
   @Override
   public void testNullOnIntegerField() throws Exception {
      super.testNullOnIntegerField();
   }

   /**
    * This test uses fields that are not marked as @NumericField so it cannot work correctly with Lucene but should work
    * correctly for non-indexed.
    */
   public void testAnd5() throws Exception {
      QueryFactory qf = getQueryFactory();

      Query q = qf.from(getModelFactory().getUserImplClass())
            .having("id").lt(1000)
            .and().having("age").lt(1000)
            .toBuilder().build();

      List<User> list = q.list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }
}
