package org.infinispan.api;

import java.util.function.BiConsumer;

import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.1
 */
public abstract class BaseCacheAPIOptimisticTest extends CacheAPITest {
   @Test(dataProvider = "lockedStreamActuallyLocks", expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamActuallyLocks(BiConsumer<Cache<Object, Object>, CacheEntry<Object, Object>> consumer,
         boolean forEachOrInvokeAll) throws Throwable {
      super.testLockedStreamActuallyLocks(consumer, forEachOrInvokeAll);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamSetValue() {
      super.testLockedStreamSetValue();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamWithinLockedStream() {
      super.testLockedStreamWithinLockedStream();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamInvokeAllFilteredSet() {
      super.testLockedStreamInvokeAllFilteredSet();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamInvokeAllPut() {
      super.testLockedStreamInvokeAllPut();
   }
}
