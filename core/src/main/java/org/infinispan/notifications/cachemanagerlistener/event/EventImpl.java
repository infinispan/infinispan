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
package org.infinispan.notifications.cachemanagerlistener.event;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of cache manager events
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class EventImpl implements CacheStartedEvent, CacheStoppedEvent, ViewChangedEvent, MergeEvent {

   String cacheName;
   EmbeddedCacheManager cacheManager;
   Type type;
   List<Address> newMembers, oldMembers;
   Address localAddress;
   int viewId;
   private List<List<Address>> subgroupsMerged;
   private boolean mergeView;

   public EventImpl() {
   }

   public EventImpl(String cacheName, EmbeddedCacheManager cacheManager, Type type, List<Address> newMemberList, List<Address> oldMemberList, Address localAddress, int viewId) {
      this.cacheName = cacheName;
      this.cacheManager = cacheManager;
      this.type = type;
      this.newMembers = newMemberList;
      this.oldMembers = oldMemberList;
      this.localAddress = localAddress;
      this.viewId = viewId;
   }

   @Override
   public String getCacheName() {
      return cacheName;
   }

   public void setCacheName(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   public void setCacheManager(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public Type getType() {
      return type;
   }

   public void setType(Type type) {
      this.type = type;
   }

   @Override
   public List<Address> getNewMembers() {
      if(newMembers != null){
         return newMembers;
      }
      return Collections.emptyList();
   }

   public void setNewMembers(List<Address> newMembers) {
      this.newMembers = newMembers;
   }

   public void setOldMembers(List<Address> oldMembers) {
      this.oldMembers = oldMembers;
   }

   @Override
   public List<Address> getOldMembers() {
      if(oldMembers != null){
         return oldMembers;
      }
      return Collections.emptyList();
   }

   @Override
   public Address getLocalAddress() {
      return localAddress;
   }

   @Override
   public int getViewId() {
      return viewId;
   }

   public void setViewId(int viewId) {
      this.viewId = viewId;
   }

   public void setLocalAddress(Address localAddress) {
      this.localAddress = localAddress;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EventImpl event = (EventImpl) o;

      if (viewId != event.viewId) return false;
      if (cacheName != null ? !cacheName.equals(event.cacheName) : event.cacheName != null) return false;
      if (localAddress != null ? !localAddress.equals(event.localAddress) : event.localAddress != null) return false;
      if (newMembers != null ? !newMembers.equals(event.newMembers) : event.newMembers != null) return false;
      if (oldMembers != null ? !oldMembers.equals(event.oldMembers) : event.oldMembers != null) return false;
      if (!Util.safeEquals(subgroupsMerged, event.subgroupsMerged)) return false;
      if (type != event.type) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = cacheName != null ? cacheName.hashCode() : 0;
      result = 31 * result + (type != null ? type.hashCode() : 0);
      result = 31 * result + (newMembers != null ? newMembers.hashCode() : 0);
      result = 31 * result + (oldMembers != null ? oldMembers.hashCode() : 0);
      result = 31 * result + (localAddress != null ? localAddress.hashCode() : 0);
      result = 31 * result + viewId;
      result = 31 * result + (subgroupsMerged == null ? 0 : subgroupsMerged.hashCode());
      result = 31 * result + (mergeView ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "EventImpl{" +
              "type=" + type +
              ", newMembers=" + newMembers +
              ", oldMembers=" + oldMembers +
              ", localAddress=" + localAddress +
              ", viewId=" + viewId +
              ", subgroupsMerged=" + subgroupsMerged +
              ", mergeView=" + mergeView +
              '}';
   }

   public void setSubgroupsMerged(List<List<Address>> subgroupsMerged) {
      this.subgroupsMerged = subgroupsMerged;
   }

   @Override
   public List<List<Address>> getSubgroupsMerged() {
      return this.subgroupsMerged;
   }

   @Override
   public boolean isMergeView() {
      return mergeView;
   }

   public void setMergeView(boolean b) {
        mergeView = b;
     }

}
