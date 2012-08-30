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

package org.infinispan.statetransfer;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.topology.CacheTopology;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;

//todo [anistor] remove this class and move the remaining functionality to StateConsumer
/**
 * A component that manages the state transfer when the topology of the cluster changes.
 *
 * @author Dan Berindei <dan@infinispan.org>
 * @author Mircea Markus
 * @author anistor@redhat.com
 * @since 5.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateTransferManager {

   //todo [anistor] this is inaccurate. this node does not hold state yet in current implementation
   @ManagedAttribute(description = "If true, the node has successfully joined the grid and is considered to hold state.  If false, the join process is still in progress.")
   @Metric(displayName = "Is join completed?", dataType = DataType.TRAIT)
   boolean isJoinComplete();

   /**
    * Checks if an inbound state transfer is in progress.
    */
   @ManagedAttribute(description = "Checks whether there is a pending inbound state transfer on this cluster member.")
   @Metric(displayName = "Is state transfer in progress?", dataType = DataType.TRAIT)
   boolean isStateTransferInProgress();

   /**
    * Checks if an inbound state transfer is in progress for a given key.
    *
    * @param key
    * @return
    */
   boolean isStateTransferInProgressForKey(Object key);

   CacheTopology getCacheTopology();

   void start() throws Exception;

   void stop();

   /**
    * @return {@code true} if the local node was the first to start this cache in the cluster.
    */
   boolean isLocalNodeFirst();
}
