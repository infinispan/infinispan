package org.infinispan.stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.fail;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Test to verify stream iterator exception propagation behavior for a distributed cache.
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.DistributedStreamIteratorExceptionTest")
public class DistributedStreamIteratorExceptionTest extends BaseSetupStreamIteratorTest {
   protected DistributedStreamIteratorExceptionTest(CacheMode cacheMode) {
      super(false, cacheMode);
   }

   public DistributedStreamIteratorExceptionTest() {
      this(CacheMode.DIST_SYNC);
   }

   public void ensureDataContainerRemoteExceptionPropagated() {
      Cache cache0 = cache(0, CACHE_NAME);
      Cache cache1 = cache(1, CACHE_NAME);
      // Extract real one to replace after
      InternalDataContainer dataContainer = TestingUtil.extractComponent(cache1, InternalDataContainer.class);
      try {
         Throwable t = new AssertionError();
         InternalDataContainer mockContainer = when(mock(InternalDataContainer.class).spliterator(any())).thenThrow(t).getMock();
         doThrow(t).when(mockContainer).spliterator();
         TestingUtil.replaceComponent(cache1, InternalDataContainer.class, mockContainer, true);

         try {
            cache0.entrySet().stream().iterator().hasNext();
            fail("We should have gotten a CacheException");
         } catch (CacheException e) {
            Throwable cause = e;
            while ((cause = cause.getCause()) != null) {
               if (t.getClass().isInstance(cause)) {
                  break;
               }
            }
            assertNotNull("We should have found the throwable as a cause", cause);
         }
      } finally {
         TestingUtil.replaceComponent(cache1, InternalDataContainer.class, dataContainer, true);
      }
   }
}
