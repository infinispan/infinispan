/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.cdi.event.cachemanager;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;

import javax.enterprise.event.Event;
import java.util.List;

/**
 * @author Pete Muir
 */
@Listener
public class ViewChangedAdapter extends AbstractAdapter<ViewChangedEvent> {

   public static final ViewChangedEvent EMPTY = new ViewChangedEvent() {

      @Override
      public Type getType() {
         return null;
      }

      @Override
      public EmbeddedCacheManager getCacheManager() {
         return null;
      }

      @Override
      public List<Address> getNewMembers() {
         return null;
      }

      @Override
      public List<Address> getOldMembers() {
         return null;
      }

      @Override
      public Address getLocalAddress() {
         return null;
      }

      public boolean isNeedsToRejoin() {
         return false;
      }

      @Override
      public int getViewId() {
         return 0;
      }

      @Override
      public boolean isMergeView() {
         return false;
      }
   };

   public ViewChangedAdapter(Event<ViewChangedEvent> event) {
      super(event);
   }

   @Override
   @ViewChanged
   public void fire(ViewChangedEvent payload) {
      super.fire(payload);
   }
}
