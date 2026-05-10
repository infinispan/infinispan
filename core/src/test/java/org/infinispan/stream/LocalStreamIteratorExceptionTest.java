package org.infinispan.stream;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Test to verify stream exception propagation behavior for a local cache.
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "stream.LocalStreamIteratorExceptionTest")
public class LocalStreamIteratorExceptionTest extends BaseSetupStreamIteratorTest {
   public LocalStreamIteratorExceptionTest() {
      super(false, CacheMode.LOCAL);
   }

   public void ensureDataContainerExceptionPropagated() {
      Cache<?, ?> cache = cache(0, CACHE_NAME);
      // Extract real one to replace after
      InternalDataContainer<?, ?> dataContainer = TestingUtil.extractComponent(cache, InternalDataContainer.class);
      try {
         Throwable t = new CacheException();
         InternalDataContainer<?, ?> mockContainer = when(mock(InternalDataContainer.class).publisher(any(IntSet.class))).thenThrow(t).getMock();
         TestingUtil.replaceComponent(cache, InternalDataContainer.class, mockContainer, true);

         try {
            cache.entrySet().stream().iterator().hasNext();
            fail("We should have gotten a CacheException");
         } catch (CacheException e) {
            assertSame(t, e, "We should have found the throwable as a cause");
         }
      } finally {
         TestingUtil.replaceComponent(cache, InternalDataContainer.class, dataContainer, true);
      }
   }
}
