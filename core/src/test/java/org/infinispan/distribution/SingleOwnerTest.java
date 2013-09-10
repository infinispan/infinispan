package org.infinispan.distribution;


import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import static org.testng.AssertJUnit.*;

/**
 * Test single owner distributed cache configurations.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "distribution.SingleOwnerTest")
public class SingleOwnerTest extends BaseDistFunctionalTest<Object, String> {

   @Override
   protected void createCacheManagers() throws Throwable {
      cacheName = "dist";
      configuration = getDefaultClusteredCacheConfig(sync ? CacheMode.DIST_SYNC : CacheMode.DIST_ASYNC, tx);
      if (!testRetVals) {
         configuration.unsafe().unreliableReturnValues(true);
         // we also need to use repeatable read for tests to work when we dont have reliable return values, since the
         // tests repeatedly queries changes
         configuration.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      }
      configuration.clustering().sync().replTimeout(3, TimeUnit.SECONDS);
      configuration.clustering().hash().numOwners(1);
      configuration.locking().lockAcquisitionTimeout(45, TimeUnit.SECONDS);
      caches = createClusteredCaches(2, cacheName, configuration);

      c1 = caches.get(0);
      c2 = caches.get(1);

      cacheAddresses = new ArrayList<Address>(2);
      for (Cache cache : caches) {
         EmbeddedCacheManager cacheManager = cache.getCacheManager();
         cacheAddresses.add(cacheManager.getAddress());
      }

      waitForClusterToForm(cacheName);
   }

   public void testPutOnKeyOwner() {
      Cache[] caches = getOwners("mykey", 1);
      assert caches.length == 1;
      Cache ownerCache = caches[0];
      ownerCache.put("mykey", new Object());
   }

   public void testClearOnKeyOwner() {
      Cache[] caches = getOwners("mykey", 1);
      assert caches.length == 1;
      Cache ownerCache = caches[0];
      ownerCache.clear();
   }

   public void testRetrieveNonSerializableKeyFromNonOwner() {
      Cache[] owners = getOwners("yourkey", 1);
      Cache[] nonOwners = getNonOwners("yourkey", 1);
      assert owners.length == 1;
      assert nonOwners.length == 1;
      Cache ownerCache = owners[0];
      Cache nonOwnerCache = nonOwners[0];
      ownerCache.put("yourkey", new Object());
      try {
         nonOwnerCache.get("yourkey");
         fail("Should have failed with a org.infinispan.marshall.NotSerializableException");
      } catch (NotSerializableException e) {
      }
   }

   public void testErrorWhenRetrievingKeyFromNonOwner() {
      log.trace("Before test");
      Cache[] owners = getOwners("diffkey", 1);
      Cache[] nonOwners = getNonOwners("diffkey", 1);
      assert owners.length == 1;
      assert nonOwners.length == 1;
      Cache ownerCache = owners[0];
      Cache nonOwnerCache = nonOwners[0];
      ownerCache.put("diffkey", new Externalizable() {
         private static final long serialVersionUID = -483939825697574242L;

         @Override
         public void writeExternal(ObjectOutput out) throws IOException {
            throw new UnknownError();
         }
         @Override
         public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
         }
      });
      try {
         nonOwnerCache.get("diffkey");
         assert false : "Should have failed with a CacheException that contains an UnknownError";
      } catch (CacheException e) {
         if (e.getCause() != null) {
            assertTrue(e.getCause() instanceof UnknownError);
         } else {
            throw e;
         }
      }
   }
}
