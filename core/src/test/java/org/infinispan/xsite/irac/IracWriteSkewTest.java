package org.infinispan.xsite.irac;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.util.Arrays;
import java.util.Objects;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Make sure write-skew check isn't broken.
 * <p>
 * The remote sites updates are non-transactional (forced) so the write-skew version must changed with it.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "functional", testName = "xsite.irac.IracWriteSkewTest")
public class IracWriteSkewTest extends AbstractMultipleSitesTest {
   private static final int N_SITES = 2;
   private static final int CLUSTER_SIZE = 2;
   private static final String CACHE_NAME = "ws-cache";

   private static InternalDataContainer<String, String> dataContainer(Cache<String, String> cache) {
      //noinspection unchecked
      return TestingUtil.extractComponent(cache, InternalDataContainer.class);
   }

   private static DistributionInfo distributionInfo(Cache<String, String> cache, String key) {
      return TestingUtil.extractCacheTopology(cache).getDistribution(key);
   }

   @DataProvider(name = "default")
   public Object[][] dataProvider() {
      return Arrays.stream(TestMode.values())
            .map(testMode -> new Object[]{testMode})
            .toArray(Object[][]::new);
   }

   @Test(dataProvider = "default")
   public void testWriteSkewCheck(TestMode testMode) throws Exception {
      final Cache<String, String> lonCache = cache(siteName(0), CACHE_NAME, 0);
      final Cache<String, String> nycCache = cache(siteName(1), CACHE_NAME, 0);
      final TransactionManager tm = nycCache.getAdvancedCache().getTransactionManager();
      final String key = testMode.toString();

      if (!testMode.startEmpty) {
         lonCache.put(key, "before");
         eventuallyAssertInAllSitesAndCaches(CACHE_NAME, C -> Objects.equals("before", C.get(key)));
         checkKey(key, "before");
      }

      //start a tx and read the key
      //the transaction will keep the version read
      tm.begin();
      String oldValue = nycCache.get(key);
      if (testMode.startEmpty) {
         assertNull(oldValue);
      } else {
         assertEquals("before", oldValue);
      }

      //suspend the transaction and write in LON to generate a WriteSkewException
      final Transaction tx = tm.suspend();
      if (testMode.isRemove) {
         lonCache.remove(key);
         //Make sure the entry is replicated to NYC before attempting to commit NYC transaction
         //If the transaction is committed without IRAC finishes, it generates an IRAC conflict and the LON update wins
         //It makes the assertion on line 106 to fail.
         eventually(() -> iracManager(siteName(0), CACHE_NAME, 0).isEmpty());
         eventuallyAssertInAllSitesAndCaches(CACHE_NAME, c -> Objects.isNull(c.get(key)));
      } else {
         lonCache.put(key, "write-skew-value");
         eventuallyAssertInAllSitesAndCaches(CACHE_NAME, c -> Objects.equals("write-skew-value", c.get(key)));
         checkKey(key, "write-skew-value");
      }

      tm.resume(tx);
      nycCache.put(key, "after");
      if (testMode.startEmpty && testMode.isRemove) {
         //remove non-existing in LON doesn't trigger a WriteSkewException in NYC
         //because NYC reads non-existing and after the update from LON, the key still doesn't exist.
         tm.commit();
         eventuallyAssertInAllSitesAndCaches(CACHE_NAME, c -> Objects.equals("after", c.get(key)));
         checkKey(key, "after");
      } else {
         Exceptions.expectException(RollbackException.class, tm::commit);
         if (testMode.isRemove) {
            eventuallyAssertInAllSitesAndCaches(CACHE_NAME, c -> Objects.isNull(c.get(key)));
         } else {
            eventuallyAssertInAllSitesAndCaches(CACHE_NAME, c -> Objects.equals("write-skew-value", c.get(key)));
            checkKey(key, "write-skew-value");
         }
      }
   }

   @Override
   protected int defaultNumberOfSites() {
      return N_SITES;
   }

   @Override
   protected int defaultNumberOfNodes() {
      return CLUSTER_SIZE;
   }

   @Override
   protected void afterSitesCreated() {
      //LON is non-transactional
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.sites().addBackup().site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.ASYNC);
      startCache(siteName(0), CACHE_NAME, builder);

      //NYC is transactional + write-skew
      builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      builder.sites().addBackup().site(siteName(0)).strategy(BackupConfiguration.BackupStrategy.ASYNC);
      startCache(siteName(1), CACHE_NAME, builder);
   }

   private void checkKey(String key, String value) {
      //irac version is the same in all nodes & sites. extract it from one site and check everywhere.
      IracEntryVersion iracVersion = extractIracEntryVersion(key);
      assertNotNull(iracVersion);
      assertIracEntryVersion(key, value, iracVersion);
      //NYC has the EntryVersion for write-skew check
      IncrementableEntryVersion entryVersion = extractEntryVersion(key);
      assertNotNull(entryVersion);
      assertIracEntryVersion(key, value, iracVersion, entryVersion);
   }

   private IracEntryVersion extractIracEntryVersion(String key) {
      return primaryOwner(0, key).getInternalMetadata().iracMetadata().getVersion();
   }


   private IncrementableEntryVersion extractEntryVersion(String key) {
      return primaryOwner(1, key).getInternalMetadata().entryVersion();
   }

   private InternalCacheEntry<String, String> primaryOwner(int siteIndex, String key) {
      for (Cache<String, String> c : this.<String, String>caches(siteName(siteIndex), CACHE_NAME)) {
         DistributionInfo distributionInfo = distributionInfo(c, key);
         if (distributionInfo.isPrimary()) {
            return dataContainer(c).peek(distributionInfo.segmentId(), key);
         }
      }
      fail("Unable to find primary owner for key: " + key);
      throw new IllegalStateException();
   }

   private void assertIracEntryVersion(String key, String value, IracEntryVersion version) {
      for (Cache<String, String> c : this.<String, String>caches(siteName(0), CACHE_NAME)) {
         InternalDataContainer<String, String> dc = dataContainer(c);
         InternalCacheEntry<String, String> entry = dc.peek(key);
         assertEquals(value, entry.getValue());
         assertEquals(version, entry.getInternalMetadata().iracMetadata().getVersion());
      }
   }

   private void assertIracEntryVersion(String key, String value, IracEntryVersion iracVersion,
         IncrementableEntryVersion entryVersion) {
      for (Cache<String, String> c : this.<String, String>caches(siteName(1), CACHE_NAME)) {
         InternalDataContainer<String, String> dc = dataContainer(c);
         InternalCacheEntry<String, String> entry = dc.peek(key);
         assertEquals(value, entry.getValue());
         assertEquals(iracVersion, entry.getInternalMetadata().iracMetadata().getVersion());
         assertEquals(entryVersion, entry.getInternalMetadata().entryVersion());
      }
   }

   private enum TestMode {
      EMPTY_AND_REMOVE(true, true),
      EMPTY_AND_PUT(true, false),
      NON_EMPTY_AND_REMOVE(false, true),
      NON_EMPTY_AND_PUT(false, false),
      ;
      private final boolean startEmpty;
      private final boolean isRemove;

      TestMode(boolean startEmpty, boolean isRemove) {
         this.startEmpty = startEmpty;
         this.isRemove = isRemove;
      }
   }
}
