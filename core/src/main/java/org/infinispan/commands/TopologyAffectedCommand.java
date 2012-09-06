/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.commands;

/**
 * Some of the commands sent over the wire can only be honored by the receiver if the topology of the cluster at
 * delivery time is still 'compatible' with the topology in place at send time (eg. a 'get' command cannot execute
 * on a node that is no longer owner after state transfer took place). These commands need to be tagged with
 * the current topology id of the sender so the receiver can detect and handle topology mismatches.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public interface TopologyAffectedCommand extends VisitableCommand {

   int getTopologyId();

   void setTopologyId(int topologyId);
}