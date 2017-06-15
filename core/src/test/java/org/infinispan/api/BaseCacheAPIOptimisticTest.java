package org.infinispan.api;

import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.1
 */
public abstract class BaseCacheAPIOptimisticTest extends CacheAPITest {
   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStream() throws Throwable {
      super.testLockedStream();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamSetValue() {
      super.testLockedStreamSetValue();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamFunctionalCommand() throws Throwable {
      super.testLockedStreamFunctionalCommand();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamPutAll() throws Throwable {
      super.testLockedStreamPutAll();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamWithinLockedStream() {
      super.testLockedStreamWithinLockedStream();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamPutAsync() throws Throwable {
      super.testLockedStreamPutAsync();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamCompute() throws Throwable {
      super.testLockedStreamCompute();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamComputeIfPresent() throws Throwable {
      super.testLockedStreamComputeIfPresent();
   }
}
