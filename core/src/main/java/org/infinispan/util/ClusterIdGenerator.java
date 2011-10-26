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

package org.infinispan.util;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class generates ids guaranteed to be unique cluster wide.
 *
 * @author Galder Zamarreno
 * @author Mircea Markus
 * @since 4.1
 */
public class ClusterIdGenerator {

   private static final Log log = LogFactory.getLog(ClusterIdGenerator.class);

   // TODO: Possibly seed version counter on capped System.currentTimeMillis, to avoid issues with clients holding to versions in between restarts
   private final AtomicInteger versionCounter = new AtomicInteger();

   private final AtomicLong versionPrefix = new AtomicLong();
   private RankCalculator rankCalculator = new RankCalculator();

   public ClusterIdGenerator(EmbeddedCacheManager cm, RpcManager rpcManager) {
      if (cm != null)
         cm.addListener(rankCalculator);

      if (rpcManager != null) {
         Transport transport = rpcManager.getTransport();
         calculateRank(rpcManager.getAddress(),
                       transport.getMembers(), transport.getViewId());
      }
   }

   public long newVersion(boolean isClustered) {
      if (isClustered && versionPrefix.get() == 0)
         throw new IllegalStateException("If clustered, version prefix cannot be 0. Rank calculator probably not in use.");
      long counter = versionCounter.incrementAndGet();
      // Version counter occupies the least significant 4 bytes of the version
      if (isClustered) {
         return versionPrefix.get() | counter;
      } else {
         return counter;
      }
   }

   void resetCounter() {
      versionCounter.compareAndSet(versionCounter.get(), 0);
   }

   private int findAddressRank(Address address, List<Address> members, int rank) {
      if (address.equals(members.get(0))) return rank;
      else return findAddressRank(address, members.subList(1, members.size()), rank + 1);
   }

   protected long calculateRank(Address address, List<Address> members, long viewId) {
      long rank = findAddressRank(address, members, 1);
      // Version is composed of: <view id (2 bytes)><rank (2 bytes)><version counter (4 bytes)>
      // View id and rank form the prefix which is updated on a view change.
      long newVersionPrefix = (viewId << 48) | (rank << 32);
      // TODO: Deal with compareAndSet failures?
      versionPrefix.compareAndSet(versionPrefix.get(), newVersionPrefix);
      return versionPrefix.get();
   }

   @Listener
   public class RankCalculator {

      @ViewChanged
      public void calculateRank(ViewChangedEvent e) {
         long rank = ClusterIdGenerator.this
            .calculateRank(e.getLocalAddress(), e.getNewMembers(), e.getViewId());
         if (log.isTraceEnabled())
            log.tracef("Calculated rank based on view %s and result was %d", e, rank);
      }

   }
}
