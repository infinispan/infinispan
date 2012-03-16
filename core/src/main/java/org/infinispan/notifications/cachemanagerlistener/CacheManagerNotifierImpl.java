/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.notifications.cachemanagerlistener;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.AbstractListenerImpl;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.event.EventImpl;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
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

   private static final Map<Class<? extends Annotation>, Class> allowedListeners = new HashMap<Class<? extends Annotation>, Class>(4);

   static {
      allowedListeners.put(CacheStarted.class, CacheStartedEvent.class);
      allowedListeners.put(CacheStopped.class, CacheStoppedEvent.class);
      allowedListeners.put(ViewChanged.class, ViewChangedEvent.class);
      allowedListeners.put(Merged.class, MergeEvent.class);
   }

   final List<ListenerInvocation> cacheStartedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> cacheStoppedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> viewChangedListeners = new CopyOnWriteArrayList<ListenerInvocation>();
   final List<ListenerInvocation> mergeListeners = new CopyOnWriteArrayList<ListenerInvocation>();

   private EmbeddedCacheManager cacheManager;

   public CacheManagerNotifierImpl() {
      listenersMap.put(CacheStarted.class, cacheStartedListeners);
      listenersMap.put(CacheStopped.class, cacheStoppedListeners);
      listenersMap.put(ViewChanged.class, viewChangedListeners);
      listenersMap.put(Merged.class, mergeListeners);
   }

   @Inject
   public void injectCacheManager(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

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

   protected Log getLog() {
      return log;
   }

   protected Map<Class<? extends Annotation>, Class> getAllowedMethodAnnotations() {
      return allowedListeners;
   }
}
