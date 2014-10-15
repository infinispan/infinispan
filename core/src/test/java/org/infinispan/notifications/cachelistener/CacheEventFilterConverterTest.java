package org.infinispan.notifications.cachelistener;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;

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
      CacheEventFilterConverter<Object, Object, Object> filterConverter =
            when(mock(CacheEventFilterConverter.class).filterAndConvert(anyObject(), anyObject(), any(Metadata.class),
                                                                        anyObject(), any(Metadata.class),
                                                                        any(EventType.class))).thenReturn(value).getMock();
      CacheListener listener = new CacheListener();
      cache.addListener(listener, filterConverter, filterConverter);

      cache.put("key", "value");

      assertEquals(2, listener.getInvocationCount());

      verify(filterConverter, times(2)).filterAndConvert(anyObject(), anyObject(), any(Metadata.class),
                                                         anyObject(), any(Metadata.class),
                                                         any(EventType.class));
      verify(filterConverter, never()).accept(anyObject(), anyObject(), any(Metadata.class),
                                              anyObject(), any(Metadata.class),
                                              any(EventType.class));
      verify(filterConverter, never()).convert(anyObject(), anyObject(), any(Metadata.class),
                                               anyObject(), any(Metadata.class),
                                               any(EventType.class));
   }
}
