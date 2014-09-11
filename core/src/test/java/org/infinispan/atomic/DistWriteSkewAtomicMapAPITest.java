package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.distribution.MagicKey;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.List;
import java.util.Map;

import static org.infinispan.atomic.AtomicMapLookup.getAtomicMap;
import static org.testng.AssertJUnit.fail;

/**
 * {@link org.infinispan.atomic.impl.AtomicHashMap} test with write skew check enabled in a distributed cluster.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "atomic.DistWriteSkewAtomicMapAPITest")
public class DistWriteSkewAtomicMapAPITest extends DistRepeatableReadAtomicMapAPITest {

   public void testWriteSkewOnPrimaryOwner() throws Exception {
      doWriteSkewTest(cache(0, "atomic"), new MagicKey(cache(0, "atomic"), cache(1, "atomic")), caches("atomic"));
   }

   public void testWriteSkewOnBackupOwner() throws Exception {
      doWriteSkewTest(cache(1, "atomic"), new MagicKey(cache(0, "atomic"), cache(1, "atomic")), caches("atomic"));
   }

   public void testWriteSkewOnNonOwner() throws Exception {
      doWriteSkewTest(cache(2, "atomic"), new MagicKey(cache(0, "atomic"), cache(1, "atomic")), caches("atomic"));
   }

   @Override
   public void testConcurrentWritesOnExistingMap() throws Exception {
      //no-op
      //the test is not valid since concurrent writes will throw a write skew exception.
   }

   @Override
   public void testConcurrentTx() throws Exception {
      //no-op
      //the test is unstable since we use separate threads to perform the write. in some execution, we can have a
      //write skew exception
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, "atomic", configuration());
   }

   private void doWriteSkewTest(final Cache<Object, Object> cache, final Object mapKey,
                                final List<Cache<Object, Object>> caches)
         throws Exception {
      final TransactionManager tm = tm(cache);
      Map<Object, Object> atomicMap = getAtomicMap(cache, mapKey, true);

      //let's put some values
      atomicMap.put("k1", "v1");

      tm.begin();
      AssertJUnit.assertEquals("v1", atomicMap.get("k1"));
      final Transaction tx1 = tm.suspend();

      tm.begin();
      AssertJUnit.assertEquals("v1", atomicMap.get("k1"));
      final Transaction tx2 = tm.suspend();

      tm.resume(tx1);
      atomicMap.put("k1", "v2");
      tm.commit();

      tm.resume(tx2);
      AssertJUnit.assertEquals("v1", atomicMap.get("k1"));
      atomicMap.put("k1", "v3");
      try {
         tm.commit();
         fail();
      } catch (RollbackException e) {
         //expected
         safeRollback(tm);
      }

      for (Cache<Object, Object> cache1 : caches) {
         AssertJUnit.assertEquals("v2", getAtomicMap(cache1, mapKey).get("k1"));
      }
   }

   private ConfigurationBuilder configuration() {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configurationBuilder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.OPTIMISTIC)
            .locking().lockAcquisitionTimeout(2000l)
            .isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
            .versioning().enable().scheme(VersioningScheme.SIMPLE)
            .clustering().hash().numOwners(2)
            .stateTransfer().fetchInMemoryState(false);
      return configurationBuilder;
   }

}
