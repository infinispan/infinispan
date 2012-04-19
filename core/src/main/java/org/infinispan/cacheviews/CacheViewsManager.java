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
package org.infinispan.cacheviews;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

import java.util.Map;
import java.util.Set;

/**
 * A component that manages a virtual view of the cluster for each defined cache.
 *
 * @author Dan Berindei <dan@infinispan.org>
 * @since 5.1
 */
@Scope(Scopes.GLOBAL)
public interface CacheViewsManager {

   /**
    * @return The currently installed view for the given cache.
    */
   CacheView getCommittedView(String cacheName);

   /**
    * @return The pending view for the given cache.
    */
   CacheView getPendingView(String cacheName);

   /**
    * @return The members which will (should) handle commands for a given cache.
    */
   Set<Address> getLeavers(String cacheName);

   /**
    * Start the cache.
    */
   void join(String cacheName, CacheViewListener listener) throws Exception;

   /**
    * Stop the cache.
    */
   void leave(String cacheName);


   // Remote interface: these methods are called by CacheViewControlCommand
   void handleRequestJoin(Address sender, String cacheName);

   void handleRequestLeave(Address sender, String cacheName);

   void handlePrepareView(String cacheName, CacheView pendingView, CacheView committedView) throws Exception;

   void handleCommitView(String cacheName, int viewId);

   void handleRollbackView(String cacheName, int newViewId, int committedViewId);

   /**
    * @return The last prepared view id for each cache that's running on this node.
    */
   Map<String, CacheView> handleRecoverViews();
}
