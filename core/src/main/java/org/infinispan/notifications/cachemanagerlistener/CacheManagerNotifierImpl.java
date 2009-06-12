package org.infinispan.notifications.cachemanagerlistener;

import org.infinispan.factories.annotations.Stop;
import org.infinispan.manager.CacheManager;
import org.infinispan.notifications.AbstractListenerImpl;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.EventImpl;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Global, shared notifications on the cache manager.
 *
 * @author Manik Surtani
 * @since 4.0
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

   public void notifyViewChange(List<Address> members, Address myAddress, int viewId) {
      if (!viewChangedListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setLocalAddress(myAddress);
         e.setViewId(viewId);
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
