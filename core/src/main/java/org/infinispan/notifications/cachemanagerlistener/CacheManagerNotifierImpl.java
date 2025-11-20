package org.infinispan.notifications.cachemanagerlistener;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.annotation.ConfigurationChanged;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.SiteViewChanged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ConfigurationChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.notifications.cachemanagerlistener.event.SitesViewChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.impl.ConfigurationChangedEventImpl;
import org.infinispan.notifications.cachemanagerlistener.event.impl.EventImpl;
import org.infinispan.notifications.impl.AbstractListenerImpl;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import jakarta.transaction.Transaction;

/**
 * Global, shared notifications on the cache manager.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class CacheManagerNotifierImpl extends AbstractListenerImpl<Event, ListenerInvocation<Event>>
      implements CacheManagerNotifier {

   private static final Log log = LogFactory.getLog(CacheManagerNotifierImpl.class);

   private static final Map<Class<? extends Annotation>, Class<?>> allowedListeners = new HashMap<>(4);

   static {
      allowedListeners.put(CacheStarted.class, CacheStartedEvent.class);
      allowedListeners.put(CacheStopped.class, CacheStoppedEvent.class);
      allowedListeners.put(ViewChanged.class, ViewChangedEvent.class);
      allowedListeners.put(Merged.class, MergeEvent.class);
      allowedListeners.put(ConfigurationChanged.class, ConfigurationChangedEvent.class);
      allowedListeners.put(SiteViewChanged.class, SitesViewChangedEvent.class);
   }

   final List<ListenerInvocation<Event>> cacheStartedListeners = new CopyOnWriteArrayList<>();
   final List<ListenerInvocation<Event>> cacheStoppedListeners = new CopyOnWriteArrayList<>();
   final List<ListenerInvocation<Event>> viewChangedListeners = new CopyOnWriteArrayList<>();
   final List<ListenerInvocation<Event>> mergeListeners = new CopyOnWriteArrayList<>();
   final List<ListenerInvocation<Event>> configurationChangedListeners = new CopyOnWriteArrayList<>();
   final List<ListenerInvocation<Event>> sitesViewChangedListeners = new CopyOnWriteArrayList<>();

   @Inject EmbeddedCacheManager cacheManager;

   public CacheManagerNotifierImpl() {
      listenersMap.put(CacheStarted.class, cacheStartedListeners);
      listenersMap.put(CacheStopped.class, cacheStoppedListeners);
      listenersMap.put(ViewChanged.class, viewChangedListeners);
      listenersMap.put(Merged.class, mergeListeners);
      listenersMap.put(ConfigurationChanged.class, configurationChangedListeners);
      listenersMap.put(SiteViewChanged.class, sitesViewChangedListeners);
   }

   protected class DefaultBuilder extends AbstractInvocationBuilder {

      @Override
      public ListenerInvocation<Event> build() {
         return new ListenerInvocationImpl<>(target, method, sync, classLoader, subject);
      }
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

   @Override
   public CompletionStage<Void> notifyConfigurationChanged(ConfigurationChangedEvent.EventType eventType, String entityType, String entityName, Map<String, Object> entityValue) {
      if (!configurationChangedListeners.isEmpty()) {
         ConfigurationChangedEvent e = new ConfigurationChangedEventImpl(cacheManager, eventType, entityType, entityName, entityValue);
         return invokeListeners(e, configurationChangedListeners);
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> notifyCrossSiteViewChanged(Collection<String> siteView, Collection<String> sitesUp, Collection<String> sitesDown) {
      if (sitesViewChangedListeners.isEmpty()) {
         return CompletableFutures.completedNull();
      }
      EventImpl e = new EventImpl();
      e.setType(Event.Type.SITES_VIEW_CHANGED);
      e.setCacheManager(cacheManager);
      e.setSitesView(siteView);
      e.setSitesUp(sitesUp);
      e.setSitesDown(sitesDown);
      return invokeListeners(e, sitesViewChangedListeners);
   }

   @Override
   protected void handleException(Throwable t) {
      // Only cache entry-related listeners should be able to throw an exception to veto the operation.
      // Just log the exception thrown by the invoker, it should contain all the relevant information.
      log.failedInvokingCacheManagerListener(t);
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
