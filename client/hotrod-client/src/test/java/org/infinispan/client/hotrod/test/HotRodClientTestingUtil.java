package org.infinispan.client.hotrod.test;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.EventLogListener;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Utility methods for the Hot Rod client
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class HotRodClientTestingUtil {

   private static final Log log = LogFactory.getLog(HotRodClientTestingUtil.class, Log.class);

   /**
    * Kills a remote cache manager.
    *
    * @param rcm the remote cache manager instance to kill
    */
   public static void killRemoteCacheManager(RemoteCacheManager rcm) {
      try {
         if (rcm != null) rcm.stop();
      } catch (Throwable t) {
         log.warn("Error stopping remote cache manager", t);
      }
   }

   /**
    * Kills a group of remote cache managers.
    *
    * @param rcm
    *           the remote cache manager instances to kill
    */
   public static void killRemoteCacheManagers(RemoteCacheManager... rcms) {
      if (rcms != null) {
         for (RemoteCacheManager rcm : rcms) {
            try {
               if (rcm != null)
                  rcm.stop();
            } catch (Throwable t) {
               log.warn("Error stopping remote cache manager", t);
            }
         }
      }

   }

   /**
    * Kills a group of Hot Rod servers.
    *
    * @param servers the group of Hot Rod servers to kill
    */
   public static void killServers(HotRodServer... servers) {
      if (servers != null) {
         for (HotRodServer server : servers) {
            try {
               if (server != null) server.stop();
            } catch (Throwable t) {
               log.warn("Error stopping Hot Rod server", t);
            }
         }
      }
   }

   /**
    * Invoke a task using a remote cache manager. This method guarantees that
    * the remote manager used in the task will be cleaned up after the task has
    * completed, regardless of the task outcome.
    *
    * @param c task to execute
    * @throws Exception if the task fails somehow
    */
   public static void withRemoteCacheManager(RemoteCacheManagerCallable c) {
      try {
         c.call();
      } finally {
         killRemoteCacheManager(c.rcm);
      }
   }

   public static <K, V> void withClientListener(Object listener, RemoteCacheManagerCallable c) {
      RemoteCache<K, V> cache = c.rcm.getCache();
      cache.addClientListener(listener);
      try {
         c.call();
      } finally {
         cache.removeClientListener(listener);
      }
   }

   public static <K, V> void withClientListener(Object listener,
         Object[] filterFactoryParams, Object[] converterFactoryParams, RemoteCacheManagerCallable c) {
      RemoteCache<K, V> cache = c.rcm.getCache();
      cache.addClientListener(listener, filterFactoryParams, converterFactoryParams);
      try {
         c.call();
      } finally {
         cache.removeClientListener(listener);
      }
   }

   public static <K> void expectOnlyCreatedEvent(K key, EventLogListener eventListener, Cache cache) {
      expectSingleEvent(key, eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, cache);
      expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public static <K> void expectOnlyModifiedEvent(K key, EventLogListener eventListener, Cache cache) {
      expectSingleEvent(key, eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED, cache);
      expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public static <K> void expectOnlyRemovedEvent(K key, EventLogListener eventListener, Cache cache) {
      expectSingleEvent(key, eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, cache);
      expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
   }

   public static void expectNoEvents(EventLogListener eventListener) {
      expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public static void expectNoEvents(EventLogListener eventListener, ClientEvent.Type type) {
      switch (type) {
         case CLIENT_CACHE_ENTRY_CREATED:
            assertEquals(0, eventListener.createdEvents.size());
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            assertEquals(0, eventListener.modifiedEvents.size());
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            assertEquals(0, eventListener.removedEvents.size());
            break;
      }
   }

   public static <K> void expectSingleEvent(K key, EventLogListener eventListener, ClientEvent.Type type, Cache cache) {
      switch (type) {
         case CLIENT_CACHE_ENTRY_CREATED:
            ClientCacheEntryCreatedEvent createdEvent = eventListener.pollEvent(type);
            assertEquals(key, createdEvent.getKey());
            assertEquals(serverDataVersion(cache, key), createdEvent.getVersion());
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            ClientCacheEntryModifiedEvent modifiedEvent = eventListener.pollEvent(type);
            assertEquals(key, modifiedEvent.getKey());
            assertEquals(serverDataVersion(cache, key), modifiedEvent.getVersion());
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            ClientCacheEntryRemovedEvent removedEvent = eventListener.pollEvent(type);
            assertEquals(key, removedEvent.getKey());
            break;
      }
      assertEquals(0, eventListener.queue(type).size());
   }

   private static <K> long serverDataVersion(Cache cache, K key) {
      Marshaller marshaller = new GenericJBossMarshaller();
      try {
         byte[] keyBytes = marshaller.objectToByteBuffer(key);
         Metadata metadata = cache.getAdvancedCache().getCacheEntry(keyBytes).getMetadata();
         return ((NumericVersion) metadata.version()).getVersion();
      } catch (Exception e) {
         throw new AssertionError(e);
      }
   }

   public static <K> void expectUnorderedEvents(EventLogListener eventListener, ClientEvent.Type type, K... keys) {
      List<K> assertedKeys = new ArrayList<K>();
      for (int i = 0; i < keys.length; i++) {
         ClientEvent event = eventListener.pollEvent(type);
         int initialSize = assertedKeys.size();
         for (K key : keys) {
            K eventKey = null;
            switch (event.getType()) {
               case CLIENT_CACHE_ENTRY_CREATED:
                  eventKey = ((ClientCacheEntryCreatedEvent<K>) event).getKey();
                  break;
               case CLIENT_CACHE_ENTRY_MODIFIED:
                  eventKey = ((ClientCacheEntryModifiedEvent<K>) event).getKey();
                  break;
               case CLIENT_CACHE_ENTRY_REMOVED:
                  eventKey = ((ClientCacheEntryRemovedEvent<K>) event).getKey();
                  break;
            }
            checkUnorderedKeyEvent(assertedKeys, key, eventKey);
         }
         int finalSize = assertedKeys.size();
         assertEquals(initialSize + 1, finalSize);
      }
   }

   private static <K> boolean checkUnorderedKeyEvent(List<K> assertedKeys, K key, K eventKey) {
      if (key.equals(eventKey)) {
         assertFalse(assertedKeys.contains(key));
         assertedKeys.add(key);
         return true;
      }
      return false;
   }

   public static <K> void expectFailoverEvent(EventLogListener eventListener) {
      eventListener.pollEvent(ClientEvent.Type.CLIENT_CACHE_FAILOVER);
   }

}
