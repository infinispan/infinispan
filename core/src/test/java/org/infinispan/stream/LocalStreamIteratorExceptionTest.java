package org.infinispan.stream;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.DataContainer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.fail;

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
      Cache cache = cache(0, CACHE_NAME);
      // Extract real one to replace after
      DataContainer dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
      try {
         Throwable t = new CacheException();
         DataContainer mockContainer = when(mock(DataContainer.class).iterator()).thenThrow(t).getMock();
         TestingUtil.replaceComponent(cache, DataContainer.class, mockContainer, true);
         
         try {
            cache.entrySet().stream().iterator().hasNext();
            fail("We should have gotten a CacheException");
         } catch (CacheException e) {
            assertSame("We should have found the throwable as a cause", t, e);
         }
      } finally {
         TestingUtil.replaceComponent(cache, DataContainer.class, dataContainer, true);
      }
   }
}
