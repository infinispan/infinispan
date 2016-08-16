package org.infinispan.stream;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.equivalence.AnyServerEquivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Test to verify distributed stream iterator works properly when they key is a byte[] which has issues with normal
 * equality.
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = {"functional", "smoke"}, testName = "stream.DistributedStreamIteratorTxTest")
public class DistributedStreamTxEquivalenceTest extends BaseSetupStreamIteratorTest {
   public DistributedStreamTxEquivalenceTest() {
      super(true, CacheMode.DIST_SYNC);
   }

   @Override
   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      super.enhanceConfiguration(builder);
      builder.dataContainer().keyEquivalence(new AnyServerEquivalence());
   }

   enum OwnerMode {
      PRIMARY {
         @Override
         boolean accept(Object key, DistributionManager dm, Address localAddress) {
            return dm.getPrimaryLocation(key).equals(localAddress);
         }
      },
      BACKUP {
         @Override
         boolean accept(Object key, DistributionManager dm, Address localAddress) {
            List<Address> owners  = dm.locate(key);
            Iterator<Address> iter = owners.iterator();
            // Skip primary owner
            iter.next();
            while (iter.hasNext()) {
               if (localAddress.equals(iter.next())) {
                  return true;
               }
            }
            return false;
         }
      },
      NOT_OWNER {
         @Override
         boolean accept(Object key, DistributionManager dm, Address localAddress) {
            return !dm.locate(key).contains(localAddress);
         }
      };

      abstract boolean accept(Object key, DistributionManager dm, Address localAddress);
   }

   public void testByteArrayExistingTransactionOnPrimaryOwner() throws NotSupportedException,
         SystemException, SecurityException, IllegalStateException, RollbackException,
         HeuristicMixedException, HeuristicRollbackException {
      testOwner(OwnerMode.PRIMARY);
   }

   public void testByteArrayExistingTransactionOnBackupOwner() throws NotSupportedException,
           SystemException, SecurityException, IllegalStateException, RollbackException,
           HeuristicMixedException, HeuristicRollbackException {
      testOwner(OwnerMode.BACKUP);
   }

   public void testByteArrayExistingTransactionOnNonOwner() throws NotSupportedException,
           SystemException, SecurityException, IllegalStateException, RollbackException,
           HeuristicMixedException, HeuristicRollbackException {
      testOwner(OwnerMode.NOT_OWNER);
   }

   private void testOwner(OwnerMode mode) throws SystemException, NotSupportedException {
      byte[] keyBytes = "my-key".getBytes();

      Cache<byte[], String> cache = getCache(mode, keyBytes);

      cache.put(keyBytes, "my-value");
      TransactionManager tm = TestingUtil.extractComponent(cache,
              TransactionManager.class);
      tm.begin();
      try {
         // Note it is the same bytes but different instance
         byte[] keyBytes2 = "my-key".getBytes();
         cache.put(keyBytes2, "filtered-value");

         Iterator<CacheEntry<byte[], String>> iterator = cache.getAdvancedCache().cacheEntrySet().stream().iterator();
         Map<byte[], String> results = mapFromIterator(iterator);
         assertEquals(1, results.size());
      } finally {
         tm.rollback();
      }
   }

   private <K, V> Cache<K, V> getCache(OwnerMode mode, Object key) {
      List<Cache<K, V>> caches = caches(CACHE_NAME);
      for (Cache<K, V> cache : caches) {
         if (mode.accept(key, cache.getAdvancedCache().getDistributionManager(), cache.getCacheManager().getAddress())) {
            return cache;
         }
      }
      throw new IllegalStateException("No caches matched somehow!");
   }
}
