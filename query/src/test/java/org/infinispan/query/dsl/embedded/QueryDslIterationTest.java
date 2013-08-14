package org.infinispan.query.dsl.embedded;

import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.embedded.sample_domain_model.User;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test for orderBy, projections and generally iteration.
 *
 * @author rvansa@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.dsl.QueryDslIterationTest")
public class QueryDslIterationTest extends AbstractQueryDslTest {

   @BeforeMethod
   protected void populateCache() throws Exception {
      User user1 = new User();
      user1.setId(1);
      user1.setName("John");
      user1.setSurname("White");

      User user2 = new User();
      user2.setId(2);
      user2.setName("Jack");
      user2.setSurname("Black");

      User user3 = new User();
      user3.setId(3);
      user3.setName("John");
      user3.setSurname("Brown");

      User user4 = new User();
      user4.setId(4);
      user4.setName("Michael");
      user4.setSurname("Black");

      cache.put("user_" + user1.getId(), user1);
      cache.put("user_" + user2.getId(), user2);
      cache.put("user_" + user3.getId(), user3);
      cache.put("user_" + user4.getId(), user4);
   }

   public void testOrderByAsc() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.ASC).build();

      assertEquals(4, q.getResultSize());

      List<User> list = q.list();
      assertEquals(4, list.size());
      checkNamesAsc(list);
   }

   private void checkNamesAsc(List<User> list) {
      String prevName = null;
      for (User u : list) {
         assertNotNull(u.getName());
         if (prevName != null) {
            assertTrue(u.getName().compareTo(prevName) >= 0);
         }
         prevName = u.getName();
      }
   }

   public void testOrderByDesc() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .orderBy("surname", SortOrder.DESC).build();

      assertEquals(4, q.getResultSize());

      List<User> list = q.list();
      assertEquals(4, list.size());
      String prevName = null;
      for (User u : list) {
         assertNotNull(u.getSurname());
         if (prevName != null) {
            assertTrue(u.getSurname().compareTo(prevName) <= 0);
         }
         prevName = u.getSurname();
      }
   }

   public void testMaxResults() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.ASC).maxResults(2).build();

      assertEquals(4, q.getResultSize());

      List<User> list = q.list();
      assertEquals(2, list.size());
      checkNamesAsc(list);
   }

   public void testStartOffset() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.ASC).startOffset(2).build();

      assertEquals(4, q.getResultSize());

      List<User> list = q.list();
      assertEquals(2, list.size());
      checkNamesAsc(list);
   }

   public void testProjection1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .setProjection("id", "name").build();

      assertEquals(4, q.getResultSize());

      List<Object[]> list = q.list();
      assertEquals(4, list.size());
      for (Object[] u : list) {
         assertNotNull(u[1]);
         assertTrue(u[0] instanceof Integer);
      }
   }

   public void testIteration1() throws Exception {
      LuceneQuery q = getIterationQuery();
      checkIterator(4, q.iterator());
   }

   public void testIteration2() throws Exception {
      LuceneQuery q = getIterationQuery();
      checkIterator(4, q.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY).fetchSize(1)));
   }

   public void testIteration3() throws Exception {
      LuceneQuery q = getIterationQuery();
      checkIterator(4, q.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY).fetchSize(3)));
   }

   public void testIteration4() throws Exception {
      LuceneQuery q = getIterationQuery();
      checkIterator(4, q.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER).fetchSize(1)));
   }

   public void testIteration5() throws Exception {
      LuceneQuery q = getIterationQuery();
      checkIterator(4, q.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER).fetchSize(3)));
   }

   private LuceneQuery getIterationQuery() {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      QueryBuilder<LuceneQuery> queryQueryBuilder = qf.from(User.class)
            .not().having("surname").eq("Blue").toBuilder();
      return queryQueryBuilder.build();
   }

   private void checkIterator(int expected, ResultIterator iterator) {
      int elements = 0;
      while (iterator.hasNext()) {
         User u = (User) iterator.next();
         assertNotNull(u.getName());
         assertNotNull(u.getSurname());
         ++elements;
      }
      assertEquals(expected, elements);
   }
}
