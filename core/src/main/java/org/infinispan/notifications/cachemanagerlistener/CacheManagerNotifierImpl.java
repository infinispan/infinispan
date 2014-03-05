package org.infinispan.notifications.cachemanagerlistener;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.impl.EventImpl;
import org.infinispan.notifications.impl.AbstractListenerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;

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
public class CacheManagerNotifierImpl extends AbstractListenerImpl<Event, ListenerInvocation<Event>>
      implements CacheManagerNotifier {

   private static final Log log = LogFactory.getLog(CacheManagerNotifierImpl.class);

   private static final Map<Class<? extends Annotation>, Class<?>> allowedListeners = new HashMap<Class<? extends Annotation>, Class<?>>(4);

   static {
      allowedListeners.put(CacheStarted.class, CacheStartedEvent.class);
      allowedListeners.put(CacheStopped.class, CacheStoppedEvent.class);
      allowedListeners.put(ViewChanged.class, ViewChangedEvent.class);
      allowedListeners.put(Merged.class, MergeEvent.class);
   }

   final List<ListenerInvocation<Event>> cacheStartedListeners = new CopyOnWriteArrayList<ListenerInvocation<Event>>();
   final List<ListenerInvocation<Event>> cacheStoppedListeners = new CopyOnWriteArrayList<ListenerInvocation<Event>>();
   final List<ListenerInvocation<Event>> viewChangedListeners = new CopyOnWriteArrayList<ListenerInvocation<Event>>();
   final List<ListenerInvocation<Event>> mergeListeners = new CopyOnWriteArrayList<ListenerInvocation<Event>>();

   private EmbeddedCacheManager cacheManager;

   public CacheManagerNotifierImpl() {
      listenersMap.put(CacheStarted.class, cacheStartedListeners);
      listenersMap.put(CacheStopped.class, cacheStoppedListeners);
      listenersMap.put(ViewChanged.class, viewChangedListeners);
      listenersMap.put(Merged.class, mergeListeners);
   }

   protected class DefaultBuilder extends AbstractInvocationBuilder {

      @Override
      public ListenerInvocation<Event> build() {
         return new ListenerInvocationImpl(target, method, sync, classLoader, subject);
      }
   }

   @Inject
   public void injectCacheManager(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public void notifyViewChange(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId) {
      if (!viewChangedListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setLocalAddress(myAddress);
         e.setMergeView(false);
         e.setViewId(viewId);
         e.setNewMembers(members);
         e.setOldMembers(oldMembers);
         e.setCacheManager(cacheManager);
         e.setType(Event.Type.VIEW_CHANGED);
         for (ListenerInvocation listener : viewChangedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyMerge(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId, List<List<Address>> subgroupsMerged) {
      if (!mergeListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setLocalAddress(myAddress);
         e.setViewId(viewId);
         e.setMergeView(true);
         e.setNewMembers(members);
         e.setOldMembers(oldMembers);
         e.setCacheManager(cacheManager);
         e.setSubgroupsMerged(subgroupsMerged);
         e.setType(Event.Type.MERGED);
         for (ListenerInvocation listener : mergeListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyCacheStarted(String cacheName) {
      if (!cacheStartedListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setCacheName(cacheName);
         e.setCacheManager(cacheManager);
         e.setType(Event.Type.CACHE_STARTED);
         for (ListenerInvocation listener : cacheStartedListeners) listener.invoke(e);
      }
   }

   @Override
   public void notifyCacheStopped(String cacheName) {
      if (!cacheStoppedListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setCacheName(cacheName);
         e.setCacheManager(cacheManager);
         e.setType(Event.Type.CACHE_STOPPED);
         for (ListenerInvocation listener : cacheStoppedListeners) listener.invoke(e);
      }
   }

   @Override
   public void addListener(Object listener) {
      validateAndAddListenerInvocation(listener, new DefaultBuilder());
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected Map<Class<? extends Annotation>, Class<?>> getAllowedMethodAnnotations(Listener l) {
      return allowedListeners;
   }

   @Override
   protected final Transaction suspendIfNeeded() {
      return null; //no-op
   }

   @Override
   protected final void resumeIfNeeded(Transaction transaction) {
      //no-op
   }
}
