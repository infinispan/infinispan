package org.infinispan.iteration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.testng.AssertJUnit.fail;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.DataContainer;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Test to verify entry retriever exception propagation behavior for a distributed cache.
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.DistributedEntryRetrieverExceptionTest")
public class DistributedEntryRetrieverExceptionTest extends BaseSetupEntryRetrieverTest {
   public DistributedEntryRetrieverExceptionTest() {
      super(false, CacheMode.DIST_SYNC);
   }
   
   public void ensureDataContainerRemoteExceptionPropagated() {
      Cache cache0 = cache(0, CACHE_NAME);
      Cache cache1 = cache(1, CACHE_NAME);
      // Extract real one to replace after
      DataContainer dataContainer = TestingUtil.extractComponent(cache1, DataContainer.class);
      try {
         Throwable t = new AssertionError();
         DataContainer mockContainer = when(mock(DataContainer.class).iterator()).thenThrow(t).getMock();
         TestingUtil.replaceComponent(cache1, DataContainer.class, mockContainer, true);
         
         try {
            cache0.getAdvancedCache().filterEntries(AcceptAllKeyValueFilter.getInstance()).iterator().hasNext();
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
         TestingUtil.replaceComponent(cache1, DataContainer.class, dataContainer, true);
      }
   }
}
