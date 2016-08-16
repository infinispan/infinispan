package org.infinispan.notifications.cachelistener;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.notifications.cachelistener.event.Event;

/**
 * Listens to everything
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Listener
public class CacheListener {
   List<Event> events = new ArrayList<Event>();
   boolean receivedPre;
   boolean receivedPost;
   int invocationCount;

   public void reset() {
      events.clear();
      receivedPost = false;
      receivedPre = false;
      invocationCount = 0;
   }

   public List<Event> getEvents() {
      return events;
   }

   public boolean isReceivedPre() {
      return receivedPre;
   }

   public boolean isReceivedPost() {
      return receivedPost;
   }

   public int getInvocationCount() {
      return invocationCount;
   }


   // handler

   @CacheEntryActivated
   @CacheEntryCreated
   @CacheEntriesEvicted
   @CacheEntryInvalidated
   @CacheEntryLoaded
   @CacheEntryModified
   @CacheEntryPassivated
   @CacheEntryRemoved
   @CacheEntryVisited
   @TransactionCompleted
   @TransactionRegistered
   @CacheEntryExpired
   public void handle(Event e) {
      events.add(e);
      if (e.isPre())
         receivedPre = true;
      else
         receivedPost = true;

      invocationCount++;
   }
}
