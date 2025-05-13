package org.infinispan.expiration.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@Listener
public class ExpiredCacheListener {
   private static final Log log = LogFactory.getLog(ExpiredCacheListener.class);
   private final List<CacheEntryExpiredEvent> events = Collections.synchronizedList(new ArrayList<>());
   private final AtomicInteger invocationCount = new AtomicInteger();

   public void reset() {
      events.clear();
      invocationCount.set(0);
   }

   public List<CacheEntryExpiredEvent> getEvents() {
      return events;
   }

   public int getInvocationCount() {
      return invocationCount.get();
   }


   // handler

   @CacheEntryExpired
   public void handle(CacheEntryExpiredEvent e) {
      log.trace("Received event: " + e);
      events.add(e);

      invocationCount.incrementAndGet();
   }
}
