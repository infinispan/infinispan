package org.infinispan.anchored;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;

public abstract class AbstractAnchoredKeysTest extends MultipleCacheManagersTest {
   protected void assertValue(Object key, Object expectedValue) {
      for (Cache<Object, Object> cache : caches()) {
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();
         Object value = cache.get(key);
         assertEquals("Wrong value for " + key + " on " + address, expectedValue, value);
      }
   }

   protected void assertNoValue(Object key) {
      for (Cache<Object, Object> cache : caches()) {
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();
         Object value = cache.get(key);
         assertNull("Extra value for " + key + " on " + address, value);
      }
   }

   protected void assertLocation(Object key, Address expectedAddress, Object expectedValue) {
      for (Cache<Object, Object> cache : caches()) {
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();

         Object storedKey = cache.getAdvancedCache().getKeyDataConversion().toStorage(key);
         InternalCacheEntry<Object, Object> entry = cache.getAdvancedCache().getDataContainer().peek(storedKey);

         if (address.equals(expectedAddress)) {
            Object storedValue = entry != null ? entry.getValue() : null;
            Object value = cache.getAdvancedCache().getValueDataConversion().fromStorage(storedValue);
            assertEquals("Wrong value for " + key + " on " + address, expectedValue, value);
            Metadata metadata = entry != null ? entry.getMetadata() : null;
            assertFalse("No location expected for " + key + " on " + address + ", got " + metadata,
                        metadata instanceof RemoteMetadata);
         } else {
            assertNull("No value expected for key " + key + " on " + address, entry.getValue());
            Address location = ((RemoteMetadata) entry.getMetadata()).getAddress();
            assertEquals("Wrong location for " + key + " on " + address, expectedAddress, location);
         }
      }
   }

   protected void assertNoLocation(Object key) {
      List<Cache<Object, Object>> caches = caches();
      for (Cache<Object, Object> cache : caches) {
         Object storageKey = cache.getAdvancedCache().getKeyDataConversion().toStorage(key);
         InternalCacheEntry<Object, Object> entry = cache.getAdvancedCache().getDataContainer().peek(storageKey);
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();
         assertNull("Expected no location on " + address, entry);
      }

   }
}
