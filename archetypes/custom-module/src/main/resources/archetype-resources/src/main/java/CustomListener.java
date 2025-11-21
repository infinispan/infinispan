package ${package};

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;

@Listener(clustered = true, observation = Listener.Observation.POST)
public class CustomListener {
   @CacheEntryCreated
   public void entryCreated(CacheEntryCreatedEvent<Object, Object> event) {
      System.out.println("entryCreated " + event);
   }

   @CacheEntryModified
   public void entryModified(CacheEntryModifiedEvent<String, String> event) {
      System.out.println("entryModified: " + event);
   }

   @CacheEntryRemoved
   public void entryRemoved(CacheEntryRemovedEvent<String, String> event) {
      System.out.println("entryRemoved " + event);
   }
}
