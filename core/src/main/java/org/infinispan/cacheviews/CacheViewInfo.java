/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.cacheviews;

import java.util.List;

import org.infinispan.remoting.MembershipArithmetic;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The cluster-wide state of a cache.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 */
public class CacheViewInfo {
   private static final Log log = LogFactory.getLog(CacheViewInfo.class);

   private final String cacheName;
   private final Object lock = new Object();

   // The last view for which the coordinator sent a COMMIT_VIEW message
   private CacheView committedView;
   // The last view for which the coordinator sent a PREPARE_VIEW message, or null if it has already been committed
   private CacheView pendingView;

   // TODO These two don't really belong to the view state, but we keep them here to avoid creating other map
   // The cache-scoped listener
   private volatile CacheViewListener listener;

   // The view installation task - only used if this node is the coordinator
   private final PendingCacheViewChanges pendingChanges;


   public CacheViewInfo(String cacheName, CacheView initialView) {
      log.tracef("%s: Initializing state, initial view is %s", cacheName, initialView);
      this.cacheName = cacheName;
      this.committedView = initialView;
      this.pendingView = null;
      this.pendingChanges = new PendingCacheViewChanges(cacheName);
   }

   public String getCacheName() {
      return cacheName;
   }

   public CacheView getCommittedView() {
      synchronized (lock) {
         return committedView;
      }
   }

   public CacheView getPendingView() {
      synchronized (lock) {
         return pendingView;
      }
   }

   /**
    * We only support one listener per cache.
    * TODO Consider moving the listener to the <tt>CacheNotifier</tt> interface.
    * @param listener
    */
   public void setListener(CacheViewListener listener) {
      this.listener = listener;
   }

   public CacheViewListener getListener() {
      return listener;
   }

   public PendingCacheViewChanges getPendingChanges() {
      return pendingChanges;
   }

   /**
    * Update the pending view.
    * It does nothing on the coordinator, since {@code createPendingView} already updated the pending view.
    */
   public void prepareView(CacheView newView) {
      log.tracef("%s: Preparing view %s", cacheName, newView);
      synchronized (lock) {
         if (pendingView != null) {
            throw new IllegalStateException(String.format("Cannot prepare new view %s on cache %s, we are currently preparing view %s",
                  newView, cacheName, pendingView));
         }

         if (committedView.getViewId() > newView.getViewId()) {
            throw new IllegalStateException(String.format("Cannot prepare new view %s on cache %s, we have already committed view %s",
                  newView, cacheName, committedView));
         }

         this.pendingView = newView;
      }
   }

   /**
    * Update the committed view
    */
   public void commitView(int viewId) {
      log.tracef("%s: Committing view %s", cacheName, viewId);
      synchronized (lock) {
         // We need to allow re-committing the same view
         if (pendingView == null && viewId == committedView.getViewId()) {
            log.tracef("%s: Re-committing view %d", cacheName, viewId);
            return;
         }

         if (pendingView == null || viewId != pendingView.getViewId())
            throw new IllegalArgumentException(String.format("Cannot commit view %d, we are currently preparing view %s", viewId, pendingView));

         committedView = pendingView;
         pendingView = null;
      }
   }

   /**
    * Discard the pending view
    */
   public void rollbackView(int newViewId, int committedViewId) {
      log.tracef("%s: Rolling back to cache view %s, new view id is %d", cacheName, committedView, newViewId);
      synchronized (lock) {
         // Before we install the first view we don't have a proper view to go to
         if (!committedView.getMembers().isEmpty() && committedViewId != committedView.getViewId()) {
            log.cacheViewRollbackIdMismatch(committedViewId, committedView.getViewId());
         }

         pendingView = null;
         committedView = new CacheView(newViewId, committedView.getMembers());
      }
   }

   boolean hasPendingView() {
      return getPendingView() != null || getPendingChanges().isViewInstallationInProgress();
   }

   /**
    * @return The list of members that are no longer present in the {@code newMembers} list.
    * Includes both committed and pending members.
    */
   public List<Address> computeLeavers(List<Address> newMembers) {
      List<Address> leavers = MembershipArithmetic.getMembersLeft(getCommittedView().getMembers(), newMembers);
      leavers.addAll(getPendingChanges().computeMissingJoiners(newMembers));
      return leavers;
   }
}
