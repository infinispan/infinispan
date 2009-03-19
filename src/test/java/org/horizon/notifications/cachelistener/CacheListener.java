package org.horizon.notifications.cachelistener;

import org.horizon.notifications.Listener;
import org.horizon.notifications.cachelistener.annotation.*;
import org.horizon.notifications.cachelistener.event.Event;

import java.util.ArrayList;
import java.util.List;

/**
 * Listens to everything
 *
 * @author Manik Surtani
 * @since 1.0
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
   @CacheEntryEvicted
   @CacheEntryInvalidated
   @CacheEntryLoaded
   @CacheEntryModified
   @CacheEntryPassivated
   @CacheEntryRemoved
   @CacheEntryVisited
   @TransactionCompleted
   @TransactionRegistered
   public void handle(Event e) {
      events.add(e);
      if (e.isPre())
         receivedPre = true;
      else
         receivedPost = true;

      invocationCount++;
   }
}
