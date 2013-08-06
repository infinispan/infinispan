package org.infinispan.query.dsl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.sample_domain_model.User;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for orderBy, projections and generally iteration
 *
 * @author rvansa@redhat.com
 * @since 6.0
 */

@Test(groups = "functional", testName = "query.dsl.QueryDslIterationTest")
public class QueryDslIterationTest extends AbstractQueryDslTest {

   @BeforeMethod
   private void populateCache() throws Exception {
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

   @Test(enabled = false, description = "OrderBy not supported yet")
   public void testOrderByAsc() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.ASCENDING).build();

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

   @Test(enabled = false, description = "orderBy not supported yet")
   public void testOrderByDesc() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .orderBy("surname", SortOrder.DESCENDING).build();

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

   @Test(enabled = false, description = "maxResults not supported yet")
   public void testMaxResults() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.ASCENDING).maxResults(2).build();

      assertEquals(2, q.getResultSize());

      List<User> list = q.list();
      assertEquals(2, list.size());
      checkNamesAsc(list);
   }

   @Test(enabled = false, description = "startOffset not supported yet")
   public void testStartOffset() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .orderBy("name", SortOrder.ASCENDING).startOffset(2).build();

      assertEquals(2, q.getResultSize());

      List<User> list = q.list();
      assertEquals(2, list.size());
      checkNamesAsc(list);
   }

   @Test(enabled = false, description = "setProjection not supported yet")
   public void testProjection1() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .setProjection("id", "name").build();

      assertEquals(4, q.getResultSize());

      List<User> list = q.list();
      assertEquals(4, list.size());
      for (User u : list) {
         assertNotNull(u.getName());
         assertTrue(u.getId() != 0);
         assertNull(u.getSurname());
      }
   }

   @Test(enabled = false, expectedExceptions = IllegalArgumentException.class, description = "setProjection not supported yet")
   public void testProjection2() throws Exception {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      Query q = qf.from(User.class)
            .setProjection("id", "name")
            .having("surname").eq("Black")
            .toBuilder().build();

      // we should not be able to query something that is not in the projection
      q.list();
   }

   public void testIteration1() throws Exception {
      Query q = getIterationQuery();
      checkIterator(4, q.iterator());
   }

   public void testIteration2() throws Exception {
      Query q = getIterationQuery();
      checkIterator(4, q.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY).fetchSize(1)));
   }

   public void testIteration3() throws Exception {
      Query q = getIterationQuery();
      checkIterator(4, q.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY).fetchSize(3)));
   }

   public void testIteration4() throws Exception {
      Query q = getIterationQuery();
      checkIterator(4, q.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER).fetchSize(1)));
   }

   public void testIteration5() throws Exception {
      Query q = getIterationQuery();
      checkIterator(4, q.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER).fetchSize(3)));
   }

   private Query getIterationQuery() {
      QueryFactory qf = Search.getSearchManager(cache).getQueryFactory();

      return qf.from(User.class)
            .not().having("surname").eq("Blue")
            .toBuilder().build();
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
