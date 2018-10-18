package org.infinispan.notifications.cachemanagerlistener;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.transaction.Transaction;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
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
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Global, shared notifications on the cache manager.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class CacheManagerNotifierImpl extends AbstractListenerImpl<Event, ListenerInvocation<Event>>
      implements CacheManagerNotifier {

   private static final Log log = LogFactory.getLog(CacheManagerNotifierImpl.class);

   private static final Map<Class<? extends Annotation>, Class<?>> allowedListeners = new HashMap<>(4);

   static {
      allowedListeners.put(CacheStarted.class, CacheStartedEvent.class);
      allowedListeners.put(CacheStopped.class, CacheStoppedEvent.class);
      allowedListeners.put(ViewChanged.class, ViewChangedEvent.class);
      allowedListeners.put(Merged.class, MergeEvent.class);
   }

   final List<ListenerInvocation<Event>> cacheStartedListeners = new CopyOnWriteArrayList<>();
   final List<ListenerInvocation<Event>> cacheStoppedListeners = new CopyOnWriteArrayList<>();
   final List<ListenerInvocation<Event>> viewChangedListeners = new CopyOnWriteArrayList<>();
   final List<ListenerInvocation<Event>> mergeListeners = new CopyOnWriteArrayList<>();

   @Inject private EmbeddedCacheManager cacheManager;

   public CacheManagerNotifierImpl() {
      listenersMap.put(CacheStarted.class, cacheStartedListeners);
      listenersMap.put(CacheStopped.class, cacheStoppedListeners);
      listenersMap.put(ViewChanged.class, viewChangedListeners);
      listenersMap.put(Merged.class, mergeListeners);
   }

   protected class DefaultBuilder extends AbstractInvocationBuilder {

      @Override
      public ListenerInvocation<Event> build() {
         return new ListenerInvocationImpl<>(target, method, sync, classLoader, subject);
      }
   }

   private CompletionStage<Void> invokeListeners(EventImpl event, List<ListenerInvocation<Event>> listeners) {
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      for (ListenerInvocation<Event> listener : listeners) {
         aggregateCompletionStage = composeStageIfNeeded(aggregateCompletionStage, invokeListener(listener, event));
      }
      if (aggregateCompletionStage != null) {
         return aggregateCompletionStage.freeze();
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyViewChange(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId) {
      if (!viewChangedListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setLocalAddress(myAddress);
         e.setMergeView(false);
         e.setViewId(viewId);
         e.setNewMembers(members);
         e.setOldMembers(oldMembers);
         e.setCacheManager(cacheManager);
         e.setType(Event.Type.VIEW_CHANGED);
         return invokeListeners(e, viewChangedListeners);
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyMerge(List<Address> members, List<Address> oldMembers, Address myAddress, int viewId, List<List<Address>> subgroupsMerged) {
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
         return invokeListeners(e, mergeListeners);
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyCacheStarted(String cacheName) {
      if (!cacheStartedListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setCacheName(cacheName);
         e.setCacheManager(cacheManager);
         e.setType(Event.Type.CACHE_STARTED);
         return invokeListeners(e, cacheStartedListeners);
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyCacheStopped(String cacheName) {
      if (!cacheStoppedListeners.isEmpty()) {
         EventImpl e = new EventImpl();
         e.setCacheName(cacheName);
         e.setCacheManager(cacheManager);
         e.setType(Event.Type.CACHE_STOPPED);
         return invokeListeners(e, cacheStoppedListeners);
      }
      return CompletableFutures.completedNull();
   }

   private void handleException(Throwable t) {
      // Only cache entry-related listeners should be able to throw an exception to veto the operation.
      // Just log the exception thrown by the invoker, it should contain all the relevant information.
      log.failedInvokingCacheManagerListener(t);
   }

   private CompletionStage<Void> invokeListener(ListenerInvocation<Event> listener, EventImpl e) {
      try {
         CompletionStage<Void> stage = listener.invoke(e);
         if (stage != null && !CompletionStages.isCompletedSuccessfully(stage)) {
            return stage.exceptionally(t -> {
               handleException(t);
               return null;
            });
         }
      } catch (Exception x) {
         handleException(x);
      }
      return null;
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      validateAndAddListenerInvocations(listener, new DefaultBuilder());
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      removeListenerFromMaps(listener);
      return CompletableFutures.completedNull();
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

   public void start() {
      // no-op
   }
}
