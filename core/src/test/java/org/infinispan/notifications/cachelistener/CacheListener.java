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
package org.infinispan.notifications.cachelistener;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.*;
import org.infinispan.notifications.cachelistener.event.Event;

import java.util.ArrayList;
import java.util.List;

/**
 * Listens to everything
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Listener
public class CacheListener {
   List<Event> events = new ArrayList<Event>();
   boolean receivedPre;
   boolean receivedPost;
   int invocationCount;

   public void reset() {
      events.clear();
      receivedPost = false;
      receivedPre = false;
      invocationCount = 0;
   }

   public List<Event> getEvents() {
      return events;
   }

   public boolean isReceivedPre() {
      return receivedPre;
   }

   public boolean isReceivedPost() {
      return receivedPost;
   }

   public int getInvocationCount() {
      return invocationCount;
   }


   // handler

   @CacheEntryActivated
   @CacheEntryCreated
   @CacheEntriesEvicted
   @CacheEntryInvalidated
   @CacheEntryLoaded
   @CacheEntryModified
   @CacheEntryPassivated
   @CacheEntryRemoved
   @CacheEntryVisited
   @TransactionCompleted
   @TransactionRegistered
   public void handle(Event e) {
      events.add(e);
      if (e.isPre())
         receivedPre = true;
      else
         receivedPost = true;

      invocationCount++;
   }
}
