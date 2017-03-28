package org.infinispan.server.hotrod.event;

import static org.infinispan.server.hotrod.event.AbstractHotRodClusterEventsTest.addLengthPrefix;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.withClientListener;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.Test;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "server.hotrod.event.HotRodCustomEventsTest")
public class HotRodCustomEventsTest extends HotRodSingleNodeTest {

   @Override
   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      HotRodServer server = HotRodTestingUtil.startHotRodServer(cacheManager);
      server.addCacheEventConverterFactory("static-converter-factory", new StaticConverterFactory());
      server.addCacheEventConverterFactory("dynamic-converter-factory", new DynamicConverterFactory());
      return server;
   }

   public void testCustomEvents(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(),
                         Optional.of(new KeyValuePair<>("static-converter-factory", Collections.emptyList())), () -> {
               eventListener.expectNoEvents(Optional.empty());
               byte[] key = k(m);
               client().remove(key);
               eventListener.expectNoEvents(Optional.empty());
               byte[] value = v(m);
               client().put(key, 0, 0, value);
               eventListener.expectSingleCustomEvent(cache, addLengthPrefix(key, value));
               byte[] value2 = v(m, "v2-");
               client().put(key, 0, 0, value2);
               eventListener.expectSingleCustomEvent(cache, addLengthPrefix(key, value2));
               client().remove(key);
               eventListener.expectSingleCustomEvent(cache, addLengthPrefix(key));
            });
   }

   public void testParameterBasedConversion(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] customConvertKey = new byte[]{4, 5, 6};
      withClientListener(client(), eventListener, Optional.empty(), Optional.of(
            new KeyValuePair<>("dynamic-converter-factory", Collections.singletonList(new byte[]{4, 5, 6}))), () -> {
         eventListener.expectNoEvents(Optional.empty());
         byte[] key = k(m);
         byte[] value = v(m);
         client().put(key, 0, 0, value);
         eventListener.expectSingleCustomEvent(cache, addLengthPrefix(key, value));
         byte[] value2 = v(m, "v2-");
         client().put(key, 0, 0, value2);
         eventListener.expectSingleCustomEvent(cache, addLengthPrefix(key, value2));
         client().remove(key);
         eventListener.expectSingleCustomEvent(cache, addLengthPrefix(key));
         client().put(customConvertKey, 0, 0, value);
         eventListener.expectSingleCustomEvent(cache, addLengthPrefix(customConvertKey));
      });
   }

   public void testConvertedEventsNoReplay(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] key = new byte[]{1};
      byte[] value = new byte[]{2};
      client().put(key, 0, 0, value);
      withClientListener(client(), eventListener, Optional.empty(),
                         Optional.of(new KeyValuePair<>("static-converter-factory", Collections.emptyList())), () -> {
               eventListener.expectNoEvents(Optional.empty());
            });
   }

   public void testConvertedEventsReplay(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] key = new byte[]{1};
      byte[] value = new byte[]{2};
      client().put(key, 0, 0, value);
      withClientListener(client(), eventListener, Optional.empty(), Optional
            .of(new KeyValuePair<>("static-converter-factory", Collections.emptyList())), true, true, () -> {
         eventListener.expectSingleCustomEvent(cache, addLengthPrefix(key, value));
      });
   }

   private static class StaticConverterFactory implements CacheEventConverterFactory {
      @Override
      public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
         return (CacheEventConverter<K, V, C>) (CacheEventConverter<byte[], byte[], byte[]>) (key, oldValue, oldMetadata, newValue, newMetadata, eventType) -> {
            if (newValue == null)
               return addLengthPrefix(key);
            else
               return addLengthPrefix(key, newValue);
         };
      }
   }

   class DynamicConverterFactory implements CacheEventConverterFactory {
      @Override
      public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
         return (CacheEventConverter<K, V, C>) (CacheEventConverter<byte[], byte[], byte[]>) (key, oldValue, oldMetadata, newValue, newMetadata, eventType) -> {
            if (newValue == null || Arrays.equals((byte[]) params[0], key))
               return addLengthPrefix(key);
            else
               return addLengthPrefix(key, newValue);
         };
      }
   }

}
