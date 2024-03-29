package org.infinispan.distribution;


import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.RemoteException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.data.BrokenMarshallingPojo;
import org.infinispan.configuration.cache.IsolationLevel;
import org.testng.annotations.Test;

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
      configuration = getDefaultClusteredCacheConfig(cacheMode, transactional);
      if (!testRetVals) {
         configuration.unsafe().unreliableReturnValues(true);
         // we also need to use repeatable read for tests to work when we dont have reliable return values, since the
         // tests repeatedly queries changes
         configuration.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      }
      configuration.clustering().remoteTimeout(3, TimeUnit.SECONDS);
      configuration.clustering().hash().numOwners(1);
      configuration.locking().lockAcquisitionTimeout(45, TimeUnit.SECONDS);
      createClusteredCaches(2, cacheName, configuration);

      caches = caches(cacheName);
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

   public void testRetrieveNonSerializableValueFromNonOwner() {
      Cache[] owners = getOwners("yourkey", 1);
      Cache[] nonOwners = getNonOwners("yourkey", 1);
      assert owners.length == 1;
      assert nonOwners.length == 1;
      Cache ownerCache = owners[0];
      Cache nonOwnerCache = nonOwners[0];
      ownerCache.put("yourkey", new Object());
      try {
         nonOwnerCache.get("yourkey");
         fail("Should have failed with a org.infinispan.commons.marshall.MarshallingException");
      } catch (RemoteException e) {
         assertTrue(e.getCause() instanceof MarshallingException);
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
      ownerCache.put("diffkey", new BrokenMarshallingPojo());
      Exceptions.expectException(RemoteException.class, MarshallingException.class, () -> nonOwnerCache.get("diffkey"));
   }
}
