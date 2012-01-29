/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
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
package org.infinispan.configuration.cache;

public interface ClusteringConfigurationChildBuilder extends ConfigurationChildBuilder {

   /**
    * If configured all communications are asynchronous, in that whenever a thread sends a message
    * sent over the wire, it does not wait for an acknowledgment before returning. Asynchronous
    * configuration is mutually exclusive with synchronous configuration.
    */
   AsyncConfigurationBuilder async();

   /**
    * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
    */
   HashConfigurationBuilder hash();

   /**
    * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes,
    * this element is ignored.
    */
   L1ConfigurationBuilder l1();

   /**
    * Configures how state is transferred when a new cache joins the cluster.
    * Used with distribution and replication clustered modes.
    */
   StateTransferConfigurationBuilder stateTransfer();

   /**
    * If configured all communications are synchronous, in that whenever a thread sends a message
    * sent over the wire, it blocks until it receives an acknowledgment from the recipient.
    * SyncConfig is mutually exclusive with the AsyncConfig.
    */
   SyncConfigurationBuilder sync();
}
