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

import java.util.concurrent.TimeUnit;

import org.infinispan.config.ConfigurationException;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.ReplicationQueueImpl;

/**
 * If configured all communications are asynchronous, in that whenever a thread sends a message sent
 * over the wire, it does not wait for an acknowledgment before returning. Asynchronous configuration is mutually
 * exclusive with synchronous configuration.
 *
 */
public class AsyncConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<AsyncConfiguration> {

   private boolean asyncMarshalling = false;
   private ReplicationQueue replicationQueue = new ReplicationQueueImpl();
   private long replicationQueueInterval = TimeUnit.SECONDS.toMillis(5);
   private int replicationQueueMaxElements = 1000;
   private boolean useReplicationQueue = false;

   protected AsyncConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enable asynchronous marshalling. This allows the caller to return even quicker, but it can
    * suffer from reordering of operations. You can find more information at <a
    * href="https://docs.jboss.org/author/display/ISPN/Asynchronous+Options"
    * >https://docs.jboss.org/author/display/ISPN/Asynchronous+Options</a>.
    */
   public AsyncConfigurationBuilder asyncMarshalling() {
      this.asyncMarshalling = true;
      return this;
   }

   public AsyncConfigurationBuilder asyncMarshalling(boolean async) {
      this.asyncMarshalling = async;
      return this;
   }

   /**
    * Enables synchronous marshalling. You can find more information at <a
    * href="https://docs.jboss.org/author/display/ISPN/Asynchronous+Options"
    * >https://docs.jboss.org/author/display/ISPN/Asynchronous+Options</a>.
    */
   public AsyncConfigurationBuilder syncMarshalling() {
      this.asyncMarshalling = false;
      return this;
   }

   /**
    * The replication queue in use, by default {@link ReplicationQueueImpl}.
    * 
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    */
   public AsyncConfigurationBuilder replQueue(ReplicationQueue replicationQueue) {
      this.replicationQueue = replicationQueue;
      return this;
   }

   /**
    * If useReplQueue is set to true, this attribute controls how often the asynchronous thread used
    * to flush the replication queue runs.
    */
   public AsyncConfigurationBuilder replQueueInterval(long interval) {
      this.replicationQueueInterval = interval;
      return this;
   }

   /**
    * If useReplQueue is set to true, this attribute controls how often the asynchronous thread used
    * to flush the replication queue runs.
    */
   public AsyncConfigurationBuilder replQueueInterval(long interval, TimeUnit unit) {
      return replQueueInterval(unit.toMillis(interval));
   }

   /**
    * If useReplQueue is set to true, this attribute can be used to trigger flushing of the queue
    * when it reaches a specific threshold.
    */
   public AsyncConfigurationBuilder replQueueMaxElements(int elements) {
      this.replicationQueueMaxElements = elements;
      return this;
   }

   /**
    * If true, forces all async communications to be queued up and sent out periodically as a
    * batch.
    */
   public AsyncConfigurationBuilder useReplQueue(boolean use) {
      this.useReplicationQueue = use;
      return this;
   }

   @Override
   void validate() {
      if (useReplicationQueue && getClusteringBuilder().cacheMode().isDistributed())
         throw new ConfigurationException("Use of the replication queue is invalid when using DISTRIBUTED mode.");

      if (useReplicationQueue && getClusteringBuilder().cacheMode().isSynchronous())
         throw new ConfigurationException("Use of the replication queue is only allowed with an ASYNCHRONOUS cluster mode.");
   }

   @Override
   AsyncConfiguration create() {
      return new AsyncConfiguration(asyncMarshalling, replicationQueue, replicationQueueInterval, replicationQueueMaxElements, useReplicationQueue);
   }
   
   @Override
   public AsyncConfigurationBuilder read(AsyncConfiguration template) {
      this.asyncMarshalling = template.asyncMarshalling();
      this.replicationQueue = template.replQueue();
      this.replicationQueueInterval = template.replQueueInterval();
      this.replicationQueueMaxElements = template.replQueueMaxElements();
      this.useReplicationQueue = template.useReplQueue();

      return this;
   }

   @Override
   public String toString() {
      return "AsyncConfigurationBuilder{" +
            "asyncMarshalling=" + asyncMarshalling +
            ", replicationQueue=" + replicationQueue +
            ", replicationQueueInterval=" + replicationQueueInterval +
            ", replicationQueueMaxElements=" + replicationQueueMaxElements +
            ", useReplicationQueue=" + useReplicationQueue +
            '}';
   }

}
