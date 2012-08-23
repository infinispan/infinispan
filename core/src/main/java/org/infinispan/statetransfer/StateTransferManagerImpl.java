/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.statetransfer;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.topology.CacheTopology;

/**
 * //todo [anistor] remove this class and move the remaining functionality to StateConsumer
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateTransferManagerImpl implements StateTransferManager {

   private StateConsumer stateConsumer;

   public StateTransferManagerImpl() {
   }

   @Inject
   public void init(StateConsumer stateConsumer) {
      this.stateConsumer = stateConsumer;
   }

   @Override
   public boolean isJoinComplete() {
      return stateConsumer.getCacheTopology() != null; // TODO [anistor] this does not mean we have received a topology update or a rebalance yet
   }

   @Override
   public boolean isStateTransferInProgress() {
      // todo [anistor] this returns false until we receive the first rebalance and should actually return true if the cluster has > 1 member
      return stateConsumer.isStateTransferInProgress();
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      // todo [anistor] this returns false until we receive the first rebalance and should actually return true if the cluster has > 1 member
      return stateConsumer.isStateTransferInProgressForKey(key);
   }

   @Override
   public CacheTopology getCacheTopology() {
      return stateConsumer.getCacheTopology();
   }
}