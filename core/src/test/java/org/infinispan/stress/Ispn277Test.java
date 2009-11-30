/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.stress;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Ispn277Test.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "stress.Ispn277Test", enabled = false)
public class Ispn277Test extends SingleCacheManagerTest {
   private static final int USER_COUNT = 5;
   private static final int ITERATION_COUNT = 150;
   private static final int THINK_TIME_MILLIS = 10;
   private static final long LAUNCH_INTERVAL_MILLIS = 10;

   private static volatile boolean TERMINATE_ALL_USERS = false;

   private static final Random random = new Random();
   
   private Cache customerCache;
   private Cache contactCache;
//   private Cache contactsCollectionCache;
//   private Cache queryCache;
//   private Cache timestampsCache;
   
   private Set<Integer> customerIDs = new HashSet<Integer>();

   private AtomicInteger contactCounter = new AtomicInteger();

   @Override
   protected CacheManager createCacheManager() throws Exception {
      CacheManager manager = TestCacheManagerFactory.createLocalCacheManager(true);
      Configuration c = TestCacheManagerFactory.getDefaultConfiguration(true);
      c.setIsolationLevel(IsolationLevel.READ_COMMITTED);
      c.setConcurrencyLevel(1000);
      c.setLockAcquisitionTimeout(15000);
      c.setEvictionStrategy(EvictionStrategy.LRU);
      c.setEvictionWakeUpInterval(5000);
      c.setEvictionMaxEntries(1000);
      c.setExpirationMaxIdle(100000);
      c.setUseLazyDeserialization(true);
      manager.defineConfiguration("org.hibernate.test.cache.infinispan.functional.Customer", c);
      customerCache = manager.getCache("org.hibernate.test.cache.infinispan.functional.Customer");

//      manager.defineConfiguration("org.hibernate.test.cache.infinispan.functional.Customer.contacts", c.clone());
//      contactsCollectionCache = manager.getCache("org.hibernate.test.cache.infinispan.functional.Customer.contacts");

      manager.defineConfiguration("org.hibernate.test.cache.infinispan.functional.Contact", c.clone());
      contactCache = manager.getCache("org.hibernate.test.cache.infinispan.functional.Contact");
      
//      c = TestCacheManagerFactory.getDefaultConfiguration(true);
//      c.setIsolationLevel(IsolationLevel.READ_COMMITTED);
//      c.setConcurrencyLevel(1000);
//      c.setLockAcquisitionTimeout(15000);
//      c.setEvictionStrategy(EvictionStrategy.LRU);
//      c.setEvictionWakeUpInterval(5000);
//      c.setEvictionMaxEntries(1000);
//      c.setExpirationMaxIdle(100000);
//      manager.defineConfiguration("local-query", c);
//      queryCache = manager.getCache("local-query");
//
//      c = TestCacheManagerFactory.getDefaultConfiguration(true);
//      c.setIsolationLevel(IsolationLevel.READ_COMMITTED);
//      c.setConcurrencyLevel(1000);
//      c.setLockAcquisitionTimeout(15000);
//      c.setEvictionStrategy(EvictionStrategy.NONE);
//      c.setUseLazyDeserialization(true);
//      manager.defineConfiguration("timestamps", c);
//      timestampsCache = manager.getCache("timestamps");

      return manager;
   }

   public void test000() throws Throwable {
      try {
         // setup - create users
         for (int i = 0; i < USER_COUNT; i++) {
            Customer customer = createCustomer(0, i);
            getCustomerIDs().add(customer.getId());
         }
         assert USER_COUNT == getCustomerIDs().size() : "failed to create enough Customers";

         final ExecutorService executor = Executors.newFixedThreadPool(USER_COUNT);

         CyclicBarrier barrier = new CyclicBarrier(USER_COUNT + 1);
         List<Future<Void>> futures = new ArrayList<Future<Void>>(USER_COUNT);
         for (Integer customerId : getCustomerIDs()) {
            Future<Void> future = executor.submit(new UserRunner(customerId, barrier));
            futures.add(future);
            Thread.sleep(LAUNCH_INTERVAL_MILLIS); // rampup
         }
//         barrier.await(); // wait for all threads to be ready
         barrier.await(45, TimeUnit.SECONDS); // wait for all threads to finish
         log.info("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
         for (Future<Void> future : futures) future.get();
         log.info("All future gets checked");
      } catch (Throwable t) {
         log.error("Error running test", t);
         throw t;
      }

   }

   public Set<Integer> getCustomerIDs() {
      return customerIDs;
   }

   private Customer createCustomer(int nameSuffix, int id) throws Exception {
      Customer newCustomer = null;
//      try {
         newCustomer = new Customer();
         newCustomer.setName("customer_" + nameSuffix);
         newCustomer.setContacts(new HashSet<Contact>());
         newCustomer.setId(id);
         customerCache.put("org.hibernate.test.cache.infinispan.functional.Customer#" + newCustomer.getId(), newCustomer);
         // getEnvironment().getSessionFactory().getCurrentSession().persist(customer);
//      } catch (Exception e) {
//         setRollbackOnlyTx(e);
//      } finally {
//         commitOrRollbackTx();
//      }
      return newCustomer;
   }

//   protected void beginTx() throws Exception {
//      tm.begin();
//   }
//
//   protected void setRollbackOnlyTx() throws Exception {
//      tm.setRollbackOnly();
//   }
//
//   protected void setRollbackOnlyTx(Exception e) throws Exception {
//      log.error("Error", e);
//      tm.setRollbackOnly();
//      throw e;
//   }
//
//   protected void setRollbackOnlyTxExpected(Exception e) throws Exception {
//      log.debug("Expected behaivour", e);
//      tm.setRollbackOnly();
//   }
//
//   protected void commitOrRollbackTx() throws Exception {
//      if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
//      else tm.rollback();
//   }

   private Contact getFirstContact(Integer customerId) throws Exception {
      assert customerId != null;
      Contact firstContact = null;
//      beginTx();
//      try {
         final Customer loadedCustomer = (Customer) customerCache.get("org.hibernate.test.cache.infinispan.functional.Customer#" + customerId);
         Set<Contact> contacts = loadedCustomer.getContacts();
         firstContact = contacts.isEmpty() ? null : contacts.iterator().next();
//         if (firstContact != null) {
//            if (contactsCollectionCache.get("org.hibernate.test.cache.infinispan.functional.Customer.contacts#" + firstContact.getId()) == null) {
//               contactsCollectionCache.put("org.hibernate.test.cache.infinispan.functional.Customer.contacts#" + firstContact.getId(), firstContact);
//            }
//         }

//         if (TERMINATE_ALL_USERS)
//            setRollbackOnlyTx();
//      } catch (Exception e) {
//         setRollbackOnlyTx(e);
//      } finally {
//         commitOrRollbackTx();
//      }
      return firstContact;
   }

   private Contact addContact(Integer customerId) throws Exception {
      assert customerId != null;
      Contact contact = null;
//      beginTx();
//      try {
         final Customer newCustomerWithContact = (Customer) customerCache.get("org.hibernate.test.cache.infinispan.functional.Customer#" + customerId);
         contact = new Contact();
         contact.setId(contactCounter.incrementAndGet());
         contact.setName("contact name");
         contact.setTlf("wtf is tlf?");
         contact.setCustomer(newCustomerWithContact);
         newCustomerWithContact.getContacts().add(contact);
         
         contactCache.put("org.hibernate.test.cache.infinispan.functional.Contact#" + contact.getId(), contact);
//         contactsCollectionCache.put("org.hibernate.test.cache.infinispan.functional.Customer.contacts#" + contact.getId(), contact);
         // assuming contact is persisted via cascade from customer
//         if (TERMINATE_ALL_USERS)
//            setRollbackOnlyTx();
//      } catch (Exception e) {
//         setRollbackOnlyTx(e);
//      } finally {
//         commitOrRollbackTx();
//      }
      return contact;
   }

   private void readEveryonesFirstContact() throws Exception {
//      beginTx();
//      try {
         for (Integer customerId : getCustomerIDs()) {
            if (TERMINATE_ALL_USERS) {
//               setRollbackOnlyTx();
               return;
            }
            Customer loadedCustomer = (Customer) customerCache.get("org.hibernate.test.cache.infinispan.functional.Customer#" + customerId);
            Set<Contact> contacts = loadedCustomer.getContacts();
            if (!contacts.isEmpty()) {
               Contact contact = contacts.iterator().next();
               customerCache.get("org.hibernate.test.cache.infinispan.functional.Contact#" + contact.getId());
            }
         }
//      } catch (Exception e) {
//         setRollbackOnlyTx(e);
//      } finally {
//         commitOrRollbackTx();
//      }
   }

   private void removeContact(Integer customerId) throws Exception {
      assert customerId != null;

//      beginTx();
//      try {
         Customer loadedCustomer = (Customer) customerCache.get("org.hibernate.test.cache.infinispan.functional.Customer#" + customerId);
         Set<Contact> contacts = loadedCustomer.getContacts();
         if (contacts.size() != 1) {
            throw new IllegalStateException("can't remove contact: customer id=" + customerId
                     + " expected exactly 1 contact, " + "actual count=" + contacts.size());
         }

         Contact contact = contacts.iterator().next();
         contacts.remove(contact);
         contact.setCustomer(null);

         // explicitly delete Contact because hbm has no 'DELETE_ORPHAN' cascade?
         // getEnvironment().getSessionFactory().getCurrentSession().delete(contact); //appears to
         // not be needed

         // assuming contact is persisted via cascade from customer

//         if (TERMINATE_ALL_USERS)
//            setRollbackOnlyTx();
//      } catch (Exception e) {
//         setRollbackOnlyTx(e);
//      } finally {
//         commitOrRollbackTx();
//      }
   }

   private void thinkRandomTime() {
      try {
         Thread.sleep(random.nextInt(THINK_TIME_MILLIS));
      } catch (InterruptedException ex) {
         throw new RuntimeException("sleep interrupted", ex);
      }

      if (TERMINATE_ALL_USERS) {
         throw new RuntimeException("told to terminate (because a UserRunner had failed)");
      }
   }

   public static String getStackTrace(Throwable throwable) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw, true);
      throwable.printStackTrace(pw);
      return sw.getBuffer().toString();
   }

   class UserRunner implements Callable<Void> {
      private final CyclicBarrier barrier;
      final private Integer customerId;
      private int completedIterations = 0;
      private Throwable causeOfFailure;

      public UserRunner(Integer cId, CyclicBarrier barrier) {
         assert cId != null;
         this.customerId = cId;
         this.barrier = barrier;
      }

      private boolean contactExists() throws Exception {
         return getFirstContact(customerId) != null;
      }

      public Void call() throws Exception {
         // name this thread for easier log tracing
         Thread.currentThread().setName("UserRunnerThread-" + getCustomerId());
         log.info("Wait for all executions paths to be ready to perform calls");
         try {
//            barrier.await();
            for (int i = 0; i < ITERATION_COUNT && !TERMINATE_ALL_USERS; i++) {
               if (contactExists())
                  throw new IllegalStateException("contact already exists before add, customerId=" + customerId);
               addContact(customerId);
               thinkRandomTime();
               if (!contactExists())
                  throw new IllegalStateException("contact missing after successful add, customerId=" + customerId);
               thinkRandomTime();
               // read everyone's contacts
               readEveryonesFirstContact();
               thinkRandomTime();
               removeContact(customerId);
               if (contactExists())
                  throw new IllegalStateException("contact still exists after successful remove call, customerId=" + customerId);
               thinkRandomTime();
               ++completedIterations;
               if (log.isTraceEnabled()) log.trace("Iteration completed {0}", completedIterations);
            }
         } catch (Throwable t) {
            TERMINATE_ALL_USERS = true;
            log.error("Error", t);
            throw new Exception(t);
            // rollback current transaction if any
            // really should not happen since above methods all follow begin-commit-rollback pattern
            // try {
            // if
            // (DualNodeJtaTransactionManagerImpl.getInstance(DualNodeTestUtil.LOCAL).getTransaction()
            // != null) {
            // DualNodeJtaTransactionManagerImpl.getInstance(DualNodeTestUtil.LOCAL).rollback();
            // }
            // } catch (SystemException ex) {
            // throw new RuntimeException("failed to rollback tx", ex);
            // }
         } finally {
            log.info("Wait for all execution paths to finish");
            barrier.await();
         }
         return null;
      }

      public boolean isSuccess() {
         return ITERATION_COUNT == getCompletedIterations();
      }

      public int getCompletedIterations() {
         return completedIterations;
      }

      public Throwable getCauseOfFailure() {
         return causeOfFailure;
      }

      public Integer getCustomerId() {
         return customerId;
      }

      @Override
      public String toString() {
         return super.toString() + "[customerId=" + getCustomerId() + " iterationsCompleted="
                  + getCompletedIterations() + " completedAll=" + isSuccess() + " causeOfFailure="
                  + (this.causeOfFailure != null ? getStackTrace(causeOfFailure) : "") + "] ";
      }
   }

   public class Customer implements Serializable {
      Integer id;
      String name;

      private transient Set<Contact> contacts;

      public Customer() {
      }

      public Integer getId() {
         return id;
      }

      public void setId(Integer id) {
         this.id = id;
      }

      public String getName() {
         return name;
      }

      public void setName(String string) {
         name = string;
      }

      public Set<Contact> getContacts() {
         return contacts;
      }

      public void setContacts(Set<Contact> contacts) {
         this.contacts = contacts;
      }
   }

   public class Contact implements Serializable {
      Integer id;
      String name;
      String tlf;
      Customer customer;

      public Integer getId() {
         return id;
      }

      public void setId(Integer id) {
         this.id = id;
      }

      public String getName() {
         return name;
      }

      public void setName(String name) {
         this.name = name;
      }

      public String getTlf() {
         return tlf;
      }

      public void setTlf(String tlf) {
         this.tlf = tlf;
      }

      public Customer getCustomer() {
         return customer;
      }

      public void setCustomer(Customer customer) {
         this.customer = customer;
      }

      @Override
      public boolean equals(Object o) {
         if (o == this)
            return true;
         if (!(o instanceof Contact))
            return false;
         Contact c = (Contact) o;
         return c.id.equals(id) && c.name.equals(name) && c.tlf.equals(tlf);
      }

      @Override
      public int hashCode() {
         int result = 17;
         result = 31 * result + (id == null ? 0 : id.hashCode());
         result = 31 * result + name.hashCode();
         result = 31 * result + tlf.hashCode();
         return result;
      }

   }
   
}
