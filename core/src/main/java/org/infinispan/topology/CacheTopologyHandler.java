/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.topology;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.newstatetransfer.StateTransferManager;

/**
 * The link between {@link LocalTopologyManager} and {@link StateTransferManager}.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public interface CacheTopologyHandler {

   /**
    * Invoked when the CH has to be immediately updated because of a leave or when the state transfer has completed
    * and we have to install a permanent CH (pendingCH == null). A state transfer is not always required.
    */
   void updateConsistentHash(CacheTopology cacheTopology);

   /**
    * Invoked when state transfer has to be started.
    *
    * The caller will not consider the local rebalance done when this method returns. Instead, the handler
    * will have to call {@link LocalTopologyManager.confirmRebalance(String, int, Throwable)}
    */
   void rebalance(CacheTopology cacheTopology);
}
