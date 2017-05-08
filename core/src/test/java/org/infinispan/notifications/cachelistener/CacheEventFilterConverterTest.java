package org.infinispan.notifications.cachelistener;

import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test class that verifies the optimization for using a CacheEventFilterConverter works properly.
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.CacheEventFilterConverterTest")
public class CacheEventFilterConverterTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(true);
   }

   public void testFilterConvertedCalledOnlyOnce() {
      Object value = new Object();
      CacheEventFilterConverter<Object, Object, Object> filterConverter = mock(CacheEventFilterConverter.class);
      when(filterConverter.filterAndConvert(notNull(), any(), any(),
                                            any(), any(),
                                            any(EventType.class))).thenReturn(value);
      CacheListener listener = new CacheListener();
      cache.addListener(listener, filterConverter, filterConverter);

      cache.put("key", "value");

      assertEquals(2, listener.getInvocationCount());

      verify(filterConverter, times(2)).filterAndConvert(any(), any(), any(),
                                                         any(), any(),
                                                         any());
      verify(filterConverter, never()).accept(any(), any(), any(),
                                              any(), any(),
                                              any());
      verify(filterConverter, never()).convert(any(), any(), any(),
                                               any(), any(),
                                               any());
   }
}
