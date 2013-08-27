package org.infinispan.it.compatibility;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * Cache listener for testing purposes with dedicated counter and field for every event so it can assure
 * that correct event was fired and data is readable.
 *
 * @author Jiri Holusa [jholusa@redhat.com]
 */
@Listener
public class TestCacheListener {
   //Map is used instead of List so we could test if key is correct too
   Map<Object, Object> created = new HashMap<Object, Object>();
   Map<Object, Object> removed = new HashMap<Object, Object>();
   Map<Object, Object> modified = new HashMap<Object, Object>();
   Map<Object, Object> visited = new HashMap<Object, Object>();

   int createdCounter = 0;
   int removedCounter = 0;
   int modifiedCounter = 0;
   int visitedCounter = 0;

   @SuppressWarnings("unused")
   @CacheEntryCreated
   public void handleCreated(CacheEntryCreatedEvent e) {
      if (!e.isPre()) {
         created.put(e.getKey(), e.getValue());
         createdCounter++;
      }
   }

   @SuppressWarnings("unused")
   @CacheEntryRemoved
   public void handleRemoved(CacheEntryRemovedEvent e) {
      if (!e.isPre()) {
         removed.put(e.getKey(), e.getOldValue());
         removedCounter++;
      }
   }

   @SuppressWarnings("unused")
   @CacheEntryModified
   public void handleModified(CacheEntryModifiedEvent e) {
      if (!e.isPre()) {
         modified.put(e.getKey(), e.getValue());
         modifiedCounter++;
      }
   }

   @SuppressWarnings("unused")
   @CacheEntryVisited
   public void handleVisited(CacheEntryVisitedEvent e) {
      if (!e.isPre()) {
         visited.put(e.getKey(), e.getValue());
         visitedCounter++;
      }
   }

   void reset() {
      created.clear();
      removed.clear();
      modified.clear();
      visited.clear();

      createdCounter = 0;
      removedCounter = 0;
      modifiedCounter = 0;
      visitedCounter = 0;
   }
}
