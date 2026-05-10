package org.infinispan.anchored;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

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
         assertEquals(expectedValue, value, "Wrong value for " + key + " on " + address);
      }
   }

   protected void assertNoValue(Object key) {
      for (Cache<Object, Object> cache : caches()) {
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();
         Object value = cache.get(key);
         assertNull(value, "Extra value for " + key + " on " + address);
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
            assertEquals(expectedValue, value, "Wrong value for " + key + " on " + address);
            Metadata metadata = entry != null ? entry.getMetadata() : null;
            assertFalse(metadata instanceof RemoteMetadata, "No location expected for " + key + " on " + address + ", got " + metadata);
         } else {
            assertNull(entry.getValue(), "No value expected for key " + key + " on " + address);
            Address location = ((RemoteMetadata) entry.getMetadata()).getAddress();
            assertEquals(expectedAddress, location, "Wrong location for " + key + " on " + address);
         }
      }
   }

   protected void assertNoLocation(Object key) {
      List<Cache<Object, Object>> caches = caches();
      for (Cache<Object, Object> cache : caches) {
         Object storageKey = cache.getAdvancedCache().getKeyDataConversion().toStorage(key);
         InternalCacheEntry<Object, Object> entry = cache.getAdvancedCache().getDataContainer().peek(storageKey);
         Address address = cache.getAdvancedCache().getRpcManager().getAddress();
         assertNull(entry, "Expected no location on " + address);
      }

   }
}
