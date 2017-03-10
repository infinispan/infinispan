package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.MemoryConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.interceptors.impl.ContainerFullException;
import org.infinispan.interceptors.impl.TransactionalExceptionEvictionInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "eviction.ExceptionEvictionTest")
public class ExceptionEvictionTest extends MultipleCacheManagersTest {
   private static final int SIZE = 10;

   private int nodeCount;
   private StorageType storageType;
   private boolean optimisticTransaction;
   private ConfigurationBuilder configurationBuilder;

   protected ControlledTimeService timeService = new ControlledTimeService();

   public ExceptionEvictionTest nodeCount(int nodeCount) {
      this.nodeCount = nodeCount;
      return this;
   }

   public ExceptionEvictionTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   public ExceptionEvictionTest optimisticTransaction(boolean optimisticTransaction) {
      this.optimisticTransaction = optimisticTransaction;
      return this;
   }

   // Here to allow cacheMode to be method chained properly
   @Override
   public ExceptionEvictionTest cacheMode(CacheMode cacheMode) {
      super.cacheMode(cacheMode);
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.LOCAL).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true),

            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.LOCAL).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.BINARY).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.BINARY).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true),

            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.LOCAL).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.OBJECT).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(true),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.OBJECT).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(true),

            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.LOCAL).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.OFF_HEAP).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false),

            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.LOCAL).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.BINARY).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.BINARY).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.BINARY).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false),

            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.LOCAL).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(1).storageType(StorageType.OBJECT).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.OBJECT).cacheMode(CacheMode.DIST_SYNC).optimisticTransaction(false),
            new ExceptionEvictionTest().nodeCount(3).storageType(StorageType.OBJECT).cacheMode(CacheMode.REPL_SYNC).optimisticTransaction(false),
      };
   }

   static protected <T> T[] append(T[] original, T... newObjects) {
      T[] newArray = Arrays.copyOf(original, original.length + newObjects.length);
      System.arraycopy(newObjects, 0, newArray, original.length, newObjects.length);
      return newArray;
   }

   @Override
   protected String[] parameterNames() {
      return append(super.parameterNames(), "nodeCount", "storageType", "optimisticTransaction");
   }

   @Override
   protected Object[] parameterValues() {
      return append(super.parameterValues(), nodeCount, storageType, optimisticTransaction);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      configurationBuilder = new ConfigurationBuilder();
      MemoryConfigurationBuilder memoryConfigurationBuilder = configurationBuilder.memory();
      memoryConfigurationBuilder.storageType(storageType);
      memoryConfigurationBuilder.evictionStrategy(EvictionStrategy.EXCEPTION);
      switch (storageType) {
         case OBJECT:
            memoryConfigurationBuilder.size(SIZE);
            break;
         case BINARY:
            // 64 bytes per entry, however tests that add metadata require 16 more even
            memoryConfigurationBuilder.evictionType(EvictionType.MEMORY).size(convertAmountForStorage(SIZE) + 16);
            break;
         case OFF_HEAP:
            // Each entry takes up 63 bytes total for our tests, however tests that add expiration require 16 more
            memoryConfigurationBuilder.evictionType(EvictionType.MEMORY).size(24 +
                  // If we are running optimistic transactions we have to store version so it is larger than pessimistic
                  convertAmountForStorage(SIZE) +
                  UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(memoryConfigurationBuilder.addressCount() << 3));
            break;
      }
      configurationBuilder
            .transaction()
               .transactionMode(TransactionMode.TRANSACTIONAL)
               .lockingMode(optimisticTransaction ? LockingMode.OPTIMISTIC : LockingMode.PESSIMISTIC);
      configurationBuilder
            .clustering()
               .cacheMode(cacheMode)
            .hash()
               // Num owners has to be the same to guarantee amount of entries written
               .numOwners(nodeCount);

      for (int i = 0; i < nodeCount; ++i) {
         addClusterEnabledCacheManager(configurationBuilder);
      }

      for (int i = 0; i < nodeCount; ++i) {
         EmbeddedCacheManager manager = manager(i);
         TestingUtil.replaceComponent(manager, TimeService.class, timeService, true);
      }

      waitForClusterToForm();
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      super.clearContent();
      // Call actual clear to reset interceptor counter
      for (Cache cache : caches()) {
         cache.clear();
      }

      for (Cache cache : caches()) {
         long pendingTransactionCount = cache.getAdvancedCache().getAsyncInterceptorChain().findInterceptorWithClass(
               TransactionalExceptionEvictionInterceptor.class).pendingTransactionCount();
         assertEquals(0, pendingTransactionCount);
      }
   }

   Throwable getMostNestedSuppressedThrowable(Throwable t) {
      Throwable nested = getNestedThrowable(t);
      Throwable[] suppressedNested = nested.getSuppressed();
      if (suppressedNested.length > 0) {
         nested = getNestedThrowable(suppressedNested[0]);
      }
      return nested;
   }

   Throwable getNestedThrowable(Throwable t) {
      Throwable cause;
      while ((cause = t.getCause()) != null) {
         t = cause;
      }
      return t;
   }

   long convertAmountForStorage(long expected) {
      switch (storageType) {
         case OBJECT:
            return expected;
         case BINARY:
            return expected * (optimisticTransaction ? 72 : 48);
         case OFF_HEAP:
            return expected * (optimisticTransaction ? UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(51) :
                  UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(33));
         default:
            throw new IllegalStateException("Unconfigured storage type: " + storageType);
      }
   }

   /**
    * Asserts that number of entries worth of counts is stored in the interceptors
    * @param entryCount
    */
   void assertInterceptorCount(long entryCount) {
      entryCount = convertAmountForStorage(entryCount);
      long currentCount = 0;
      for (Cache cache : caches()) {
         TransactionalExceptionEvictionInterceptor interceptor = cache.getAdvancedCache().getAsyncInterceptorChain()
               .findInterceptorWithClass(TransactionalExceptionEvictionInterceptor.class);
         long size = interceptor.getCurrentSize();
         log.debugf("Exception eviction size for cache: %s is: %d", cache.getName(), size);
         currentCount += size;
         entryCount += interceptor.getMinSize();
      }

      assertEquals(entryCount, currentCount);
   }

   public void testExceptionOnInsert() {
      for (int i = 0; i < SIZE; ++i) {
         cache(0).put(i, i);
      }

      try {
         cache(0).put(-1, -1);
         fail("Should have thrown an exception!");
      } catch (Throwable t) {
         Exceptions.assertException(ContainerFullException.class, getMostNestedSuppressedThrowable(t));
      }
   }

   public void testExceptionOnInsertFunctional() {
      for (int i = 0; i < SIZE; ++i) {
         cache(0).computeIfAbsent(i, k -> SIZE);
      }

      try {
         cache(0).computeIfAbsent(-1, k -> SIZE);
         fail("Should have thrown an exception!");
      } catch (Throwable t) {
         Exceptions.assertException(ContainerFullException.class, getMostNestedSuppressedThrowable(t));
      }
   }

   public void testExceptionOnInsertWithRemove() {
      for (int i = 0; i < SIZE; ++i) {
         cache(0).put(i, i);
      }

      // Now we should have an extra space
      cache(0).remove(0);

      // Have to use a cached Integer value otherwise this will blosw up as too large
      cache(0).put(-128, -128);

      try {
         cache(0).put(-1, -1);
         fail("Should have thrown an exception!");
      } catch (Throwable t) {
         Exceptions.assertException(ContainerFullException.class, getMostNestedSuppressedThrowable(t));
      }
   }

   public void testNoExceptionWhenReplacingEntry() {
      for (int i = 0; i < SIZE; ++i) {
         cache(0).put(i, i);
      }

      // This should pass just fine
      cache(0).put(0, 0);
   }

   public void testNoExceptionAfterRollback() throws SystemException, NotSupportedException {
      // We only inserted 9
      for (int i = 1; i < SIZE; ++i) {
         cache(0).put(i, i);
      }

      assertInterceptorCount(nodeCount * (SIZE - 1));

      TransactionManager tm = cache(0).getAdvancedCache().getTransactionManager();
      tm.begin();

      cache(0).put(0, 0);

      tm.rollback();

      assertInterceptorCount(nodeCount * (SIZE - 1));

      assertNull(cache(0).get(0));

      cache(0).put(SIZE + 1, SIZE + 1);

      assertInterceptorCount(nodeCount * SIZE);

      // This should fail now
      try {
         cache(0).put(-1, -1);
         fail("Should have thrown an exception!");
      } catch (Throwable t) {
         Exceptions.assertException(ContainerFullException.class, getMostNestedSuppressedThrowable(t));
      }

      assertInterceptorCount(nodeCount * SIZE);
   }

   /**
    * This test verifies that an insert would have caused an exception, but because the user rolled back it wasn't
    * an issue
    * @throws SystemException
    * @throws NotSupportedException
    */
   public void testRollbackPreventedException() throws SystemException, NotSupportedException {
      // Insert all 10
      for (int i = 0; i < SIZE; ++i) {
         cache(0).put(i, i);
      }

      TransactionManager tm = cache(0).getAdvancedCache().getTransactionManager();
      tm.begin();

      try {
         cache(0).put(SIZE + 1, SIZE + 1);
      } finally {
         tm.rollback();
      }

      assertNull(cache(0).get(SIZE + 1));
   }

   /**
    * This test verifies that if there are multiple entries that would cause an overflow to occur. Only one entry
    * would not cause an overflow, so this is specifically for when there is more than 1.
    * @throws SystemException
    * @throws NotSupportedException
    * @throws HeuristicRollbackException
    * @throws HeuristicMixedException
    */
   public void testExceptionWithCommitMultipleEntries() throws SystemException, NotSupportedException,
         HeuristicRollbackException, HeuristicMixedException {
      // We only inserted 9
      for (int i = 1; i < SIZE; ++i) {
         cache(0).put(i, i);
      }

      TransactionManager tm = cache(0).getAdvancedCache().getTransactionManager();
      tm.begin();

      try {
         cache(0).put(0, 0);
         cache(0).put(SIZE + 1, SIZE + 1);
      } catch (Throwable t) {
         tm.setRollbackOnly();
         throw t;
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) {
            try {
               tm.commit();
               fail("Should have thrown an exception!");
            } catch (RollbackException e) {
               Exceptions.assertException(ContainerFullException.class, getMostNestedSuppressedThrowable(e));
            }
         }
         else {
            tm.rollback();
            fail("Transaction was no longer active!");
         }
      }
   }

   /**
    * This tests to verify that when an entry is expired and removed from the data container that it properly updates
    * the current count
    */
   public void testOnEntryExpiration() {
      cache(0).put(0, 0, 10, TimeUnit.SECONDS);

      for (int i = 1; i < SIZE; ++i) {
         cache(0).put(i, i);
      }

      timeService.advance(TimeUnit.SECONDS.toMillis(11));

      // This should eventually expire all entries
      assertNull(cache(0).get(0));

      // Off heap doesn't expire entries on access yet ISPN-8380
      if (storageType == StorageType.OFF_HEAP) {
         for (Cache cache : caches()) {
            ExpirationManager em = TestingUtil.extractComponent(cache, ExpirationManager.class);
            em.processExpiration();
         }
      }

      // Entry should be completely removed at some point - note that expired entries, that haven't been removed, still
      // count against counts
      for (Cache cache : caches()) {
         eventually(() -> cache.getAdvancedCache().getDataContainer().peek(0) == null);
      }

      // This insert should work now
      cache(0).put(-128, -128);

      // This should fail now
      try {
         cache(0).put(-1, -1);
         fail("Should have thrown an exception!");
      } catch (Throwable t) {
         Exceptions.assertException(ContainerFullException.class, getMostNestedSuppressedThrowable(t));
      }
   }

   public void testDistributedOverflowOnPrimary() {
      testDistributedOverflow(true);
   }

   public void testDistributedOverflowOnBackup() {
      testDistributedOverflow(false);
   }

   void testDistributedOverflow(boolean onPrimary) {
      if (!cacheMode.isDistributed() || nodeCount < 3) {
         // Ignore the test if it isn't distributed and doesn't have at least 3 nodes
         return;
      }

      // Now we add 2 more nodes which means we have 5 nodes and 3 owners
      addClusterEnabledCacheManager(configurationBuilder);
      addClusterEnabledCacheManager(configurationBuilder);

      try {
         waitForClusterToForm();

         LocalizedCacheTopology lct = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology();
         DataConversion dc = cache(0).getAdvancedCache().getKeyDataConversion();

         int minKey = -128;
         int nextKey = minKey;
         Address targetNode;
         Iterator<Address> owners = lct.getWriteOwners(dc.toStorage(nextKey)).iterator();
         if (onPrimary) {
            targetNode = owners.next();
         } else {
            // Skip first one
            owners.next();
            targetNode = owners.next();
         }


         cache(0).put(nextKey, nextKey);

         // This will fill up the cache with entries that all map to owners
         for (int i = 0; i < SIZE - 1; ++i) {
            nextKey = getNextIntWithOwners(nextKey, lct, dc, targetNode, null);
            cache(0).put(nextKey, nextKey);
         }

         // We should have interceptor count equal to number of owners times how much storage takes up
         assertInterceptorCount(nodeCount * SIZE);

         for (Cache cache : caches()) {
            if (targetNode.equals(cache.getCacheManager().getAddress())) {
               assertEquals(10, cache.getAdvancedCache().getDataContainer().size());
               break;
            }
         }

         nextKey = getNextIntWithOwners(nextKey, lct, dc, targetNode, onPrimary);
         try {
            cache(0).put(nextKey, nextKey);
            fail("Should have thrown an exception!");
         } catch (Throwable t) {
            Exceptions.assertException(ContainerFullException.class, getMostNestedSuppressedThrowable(t));
         }

         // Now that it partially failed it should have rolled back all the results
         assertInterceptorCount(nodeCount * SIZE);
      } finally {
         killMember(3);
         killMember(3);
      }
   }

   /**
    *
    * @param exclusiveValue
    * @param lct
    * @param dc
    * @param ownerAddress
    * @param primary
    * @return
    */
   int getNextIntWithOwners(int exclusiveValue, LocalizedCacheTopology lct, DataConversion dc,
         Address ownerAddress, Boolean primary) {
      if (exclusiveValue < -128) {
         throw new IllegalArgumentException("We cannot support integers smaller than -128 as they will throw off BINARY sizing");
      }

      int valueToTest = exclusiveValue;
      while (true) {
         valueToTest = valueToTest + 1;
         // Unfortunately we can't generate values higher than 128
         if (valueToTest >= 128) {
            throw new IllegalStateException("Could not generate a key with the given owners");
         }
         Object keyAsStorage = dc.toStorage(valueToTest);

         DistributionInfo di = lct.getDistribution(keyAsStorage);
         if (primary == null) {
            if (di.writeOwners().contains(ownerAddress)) {
               return valueToTest;
            }
         } else if (primary == Boolean.TRUE) {
            if (di.primary().equals(ownerAddress)) {
               return valueToTest;
            }
         } else {
            if (di.writeOwners().contains(ownerAddress)) {
               return valueToTest;
            }
         }
      }
   }

   /**
    * Test to make sure the counts are properly updated after adding and taking down nodes
    */
   public void testSizeCorrectWithStateTransfer() {
      // Test only works with REPL or DIST (latter only if numOwners > 1)
      if (!cacheMode.isClustered() || cacheMode.isDistributed() && nodeCount == 1) {
         return;
      }
      for (int i = 0; i < SIZE; ++i) {
         cache(0).put(i, i);
      }

      int numberToKill = 0;

      assertInterceptorCount(nodeCount * SIZE);

      try {
         addClusterEnabledCacheManager(configurationBuilder);

         waitForClusterToForm();

         numberToKill++;

         boolean dist = cacheMode.isDistributed();
         assertInterceptorCount((dist ? nodeCount : nodeCount + 1) * SIZE);

         addClusterEnabledCacheManager(configurationBuilder);

         waitForClusterToForm();

         numberToKill++;

         assertInterceptorCount((dist ? nodeCount : nodeCount + 2) * SIZE);

         killMember(nodeCount);

         numberToKill--;

         assertInterceptorCount((dist ? nodeCount : nodeCount + 1) * SIZE);

         killMember(nodeCount);

         numberToKill--;

         assertInterceptorCount(nodeCount * SIZE);
      } finally {
         for (int i = 0; i < numberToKill; ++i) {
            killMember(nodeCount);
         }
      }
   }
}
