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
 * Test to verify entry retriever exception propagation behavior for a local cache.
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.LocalEntryRetrieverExceptionTest")
public class LocalEntryRetrieverExceptionTest extends BaseSetupEntryRetrieverTest {
   public LocalEntryRetrieverExceptionTest() {
      super(false, CacheMode.LOCAL);
   }
   
   public void ensureDataContainerExceptionPropagated() {
      Cache cache = cache(0, CACHE_NAME);
      // Extract real one to replace after
      DataContainer dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
      try {
         Throwable t = new AssertionError();
         DataContainer mockContainer = when(mock(DataContainer.class).iterator()).thenThrow(t).getMock();
         TestingUtil.replaceComponent(cache, DataContainer.class, mockContainer, true);
         
         try {
            cache.getAdvancedCache().filterEntries(AcceptAllKeyValueFilter.getInstance()).iterator().hasNext();
            fail("We should have gotten a CacheException");
         } catch (CacheException e) {
            Throwable cause = e;
            while ((cause = cause.getCause()) != null) {
               if (cause == t) {
                  break;
               }
            }
            assertNotNull("We should have found the throwable as a cause", cause);
         }
      } finally {
         TestingUtil.replaceComponent(cache, DataContainer.class, dataContainer, true);
      }
   }
}
