package org.infinispan.test.hibernate.cache.commons.functional;

import java.util.List;

import org.hibernate.Cache;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Customer;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Item;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Diego Lovison &lt;dlovison@redhat.com&gt;
 */
public class NativeQueryTest extends SingleNodeTest {

   @Override
   public List<Object[]> getParameters() {
      return getParameters(true, true, true, true);
   }

   @Before
   public void cleanupData() throws Exception {
      super.cleanupCache();
      withTxSession(s -> {
         s.createQuery("delete Customer").executeUpdate();
         s.createQuery("delete Item").executeUpdate();
      });
   }

   @Test
   public void testEmptySecondLevelCache() throws Exception {

      final Item item = new Item( "chris", "Chris's Item" );
      Customer customer = new Customer();
      customer.setName("foo");
      withTxSession(s -> {
         s.persist(item);
         s.persist(customer);
      });

      withTxSession(s -> {
         Query q = s.createSQLQuery("update Customers set name = :name");
         q.setParameter("name", "foo");
         q.executeUpdate();
      });

      withSession(s -> {
         Cache cache = s.getSessionFactory().getCache();
         Assert.assertFalse(cache.containsEntity(Item.class, item.getId()));
         Assert.assertFalse(cache.containsEntity(Customer.class, item.getId()));
      });
   }

   @Test
   public void testAddSynchronizedEntityClass() throws Exception {

      final Item item = new Item( "chris", "Chris's Item" );
      Customer customer = new Customer();
      customer.setName("foo");
      withTxSession(s -> {
         s.persist(item);
         s.persist(customer);
      });

      withTxSession(s -> {
         Query q = s.createSQLQuery("update Customers set name = :name");
         q.setParameter("name", "foo");
         ((SQLQuery) q).addSynchronizedEntityClass(Customer.class);
         q.executeUpdate();
      });

      withSession(s -> {
         Cache cache = s.getSessionFactory().getCache();
         Assert.assertTrue(cache.containsEntity(Item.class, item.getId()));
         Assert.assertFalse(cache.containsEntity(Customer.class, item.getId()));
      });
   }
}
