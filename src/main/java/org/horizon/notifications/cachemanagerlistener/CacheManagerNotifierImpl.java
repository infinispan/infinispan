package org.horizon.notifications.cachemanagerlistener;

import org.horizon.factories.annotations.Stop;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.manager.CacheManager;
import org.horizon.notifications.AbstractListenerImpl;
import org.horizon.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.horizon.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.horizon.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.horizon.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.horizon.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.horizon.notifications.cachemanagerlistener.event.Event;
import org.horizon.notifications.cachemanagerlistener.event.EventImpl;
import org.horizon.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.horizon.remoting.transport.Address;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Global, shared notifications on the cache manager.
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class CacheManagerNotifierImpl extends AbstractListenerImpl implements CacheManagerNotifier {

   private static final Log log = LogFactory.getLog(CacheManagerNotifierImpl.class);

   private static final Map<Class<? extends Annotation>, Class> allowedListeners = new HashMap<Class<? extends Annotation>, Class>();

   static {
      allowedListeners.put(CacheStarted.class, CacheStartedEvent.class);
      allowedListeners.put(CacheStopped.class, CacheStoppedEvent.class);
      allowedListeners.put(ViewChanged.class, ViewChangedEvent.class);
   }

   final List<ListenerInvocation> cacheStartedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheStoppedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> viewChangedListeners = new CopyOnWriteArrayList<ListenerInvocation>();

   private CacheManager cacheManager;

   public CacheManagerNotifierImpl() {
      listenersMap.put(CacheStarted.class, cacheStartedListeners);
      listenersMap.put(CacheStopped.class, cacheStoppedListeners);
      listenersMap.put(ViewChanged.class, viewChangedListeners);
   }

   public void injectCacheManager(CacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   public void notifyViewChange(List<Address> members, Address myAddress) {
      if (!viewChangedListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setLocalAddress(myAddress);
         e.setNewMemberList(members);
         e.setCacheManager(cacheManager);
         e.setType(Event.Type.VIEW_CHANGED);
         for (ListenerInvocation listener : viewChangedListeners) listener.invoke(e);
      }
   }

   public void notifyCacheStarted(String cacheName) {
      if (!cacheStartedListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setCacheName(cacheName);
         e.setCacheManager(cacheManager);
         e.setType(Event.Type.CACHE_STARTED);
         for (ListenerInvocation listener : cacheStartedListeners) listener.invoke(e);
      }
   }

   public void notifyCacheStopped(String cacheName) {
      if (!cacheStoppedListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setCacheName(cacheName);
         e.setCacheManager(cacheManager);
         e.setType(Event.Type.CACHE_STOPPED);
         for (ListenerInvocation listener : cacheStoppedListeners) listener.invoke(e);
      }
   }

   @Stop
   void stop() {
      if (syncProcessor != null) syncProcessor.shutdownNow();
      if (asyncProcessor != null) asyncProcessor.shutdownNow();
   }

   protected Log getLog() {
      return log;
   }

   protected Map<Class<? extends Annotation>, Class> getAllowedMethodAnnotations() {
      return allowedListeners;
   }
}
