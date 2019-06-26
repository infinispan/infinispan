import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.*;
import org.infinispan.notifications.cachelistener.event.*;
import org.jboss.logging.Logger;

@Listener
public class LoggingListener {

   private BasicLogger log = BasicLogFactory.getLog(LoggingListener.class);

   @CacheEntryCreated
   public void observeAdd(CacheEntryCreatedEvent<String, String> event) {
      if (event.isPre())
         return;
      log.infof("Cache entry %s = %s added in cache %s", event.getKey(), event.getValue(), event.getCache());
   }

   @CacheEntryModified
   public void observeUpdate(CacheEntryModifiedEvent<String, String> event) {
      if (event.isPre())
         return;
      log.infof("Cache entry %s = %s modified in cache %s", event.getKey(), event.getValue(), event.getCache());
   }

   @CacheEntryRemoved
   public void observeRemove(CacheEntryRemovedEvent<String, String> event) {
      if (event.isPre())
         return;
      log.infof("Cache entry %s removed in cache %s", event.getKey(), event.getCache());
   }
}
