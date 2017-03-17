package org.infinispan.notifications.cachelistener;

import java.util.Set;

import org.infinispan.commons.util.Util;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.Event;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "notifications.cachelistener.CacheNotifierImplWithConverterTest")
public class CacheNotifierImplWithConverterTest extends CacheNotifierImplTest {

   @Override
   protected void addListener() {
      // Installing a listener with converter
      Set<Class> filterAnnotations = Util.asSet(CacheEntryCreated.class, CacheEntryModified.class,
            CacheEntryRemoved.class, CacheEntryExpired.class);
      n.addFilteredListener(cl, null,
            (key, oldValue, oldMetadata, newValue, newMetadata, eventType) -> "custom event for test ("
                  + eventType.getType().name() + "," + key + "," + newValue + ")",
            filterAnnotations);
   }

   @Override
   protected Object getExpectedEventValue(Object key, Object val, Event.Type t) {
      // the converter (see lambda above) returns a string with format:
      // "custom event for test (CACHE_ENTRY_<event_type>,<key>,<value>)"
      return "custom event for test (" + t.name() + "," + key + "," + val + ")";
   }
}
