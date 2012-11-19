/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;

import org.infinispan.api.BasicCacheContainer;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for {@link org.infinispan.client.hotrod.impl.operations.HotRodOperation} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class OperationsFactory implements HotRodConstants {

   private static final Flag[] FORCE_RETURN_VALUE = {Flag.FORCE_RETURN_VALUE};

   private final ThreadLocal<List<Flag>> flagsMap = new ThreadLocal<List<Flag>>();

   private final TransportFactory transportFactory;

   private final byte[] cacheNameBytes;

   private final AtomicInteger topologyId;

   private final boolean forceReturnValue;

   private final Codec codec;

   public OperationsFactory(TransportFactory transportFactory, String cacheName,
                            AtomicInteger topologyId, boolean forceReturnValue, Codec codec) {
      this.transportFactory = transportFactory;
      this.cacheNameBytes = cacheName.equals(BasicCacheContainer.DEFAULT_CACHE_NAME) ?
            DEFAULT_CACHE_NAME_BYTES : cacheName.getBytes(HOTROD_STRING_CHARSET);
      this.topologyId = topologyId;
      this.forceReturnValue = forceReturnValue;
      this.codec = codec;
   }

   public GetOperation newGetKeyOperation(byte[] key) {
      return new GetOperation(
            codec, transportFactory, key, cacheNameBytes, topologyId, flags());
   }

   public RemoveOperation newRemoveOperation(byte[] key) {
      return new RemoveOperation(
            codec, transportFactory, key, cacheNameBytes, topologyId, flags());
   }

   public RemoveIfUnmodifiedOperation newRemoveIfUnmodifiedOperation(byte[] key, long version) {
      return new RemoveIfUnmodifiedOperation(
            codec, transportFactory, key, cacheNameBytes, topologyId, flags(), version);
   }

   public ReplaceIfUnmodifiedOperation newReplaceIfUnmodifiedOperation(byte[] key,
            byte[] value, int lifespanSeconds, int maxIdleTimeSeconds, long version) {
      return new ReplaceIfUnmodifiedOperation(
            codec, transportFactory, key, cacheNameBytes, topologyId, flags(),
            value, lifespanSeconds, maxIdleTimeSeconds, version);
   }

   public GetWithVersionOperation newGetWithVersionOperation(byte[] key) {
      return new GetWithVersionOperation(
            codec, transportFactory, key, cacheNameBytes, topologyId, flags());
   }

   public StatsOperation newStatsOperation() {
      return new StatsOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags());
   }

   public PutOperation newPutKeyValueOperation(byte[] key, byte[] value,
            int lifespanSecs, int maxIdleSecs) {
      return new PutOperation(
            codec, transportFactory, key, cacheNameBytes, topologyId, flags(),
            value, lifespanSecs, maxIdleSecs);
   }

   public PutIfAbsentOperation newPutIfAbsentOperation(byte[] key, byte[] value,
            int lifespanSecs, int maxIdleSecs) {
      return new PutIfAbsentOperation(
            codec, transportFactory, key, cacheNameBytes, topologyId, flags(),
            value, lifespanSecs, maxIdleSecs);
   }

   public ReplaceOperation newReplaceOperation(byte[] key, byte[] values,
            int lifespanSecs, int maxIdleSecs) {
      return new ReplaceOperation(
            codec, transportFactory, key, cacheNameBytes, topologyId, flags(),
            values, lifespanSecs, maxIdleSecs);
   }

   public ContainsKeyOperation newContainsKeyOperation(byte[] key) {
      return new ContainsKeyOperation(
            codec, transportFactory, key, cacheNameBytes, topologyId, flags());
   }

   public ClearOperation newClearOperation() {
      return new ClearOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags());
   }

   public BulkGetOperation newBulkGetOperation(int size) {
      return new BulkGetOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags(), size);
   }

   /**
    * Construct a ping request directed to a particular node.
    *
    * @param transport represents the node to which the operation is directed
    * @return a ping operation for a particular node
    */
   public PingOperation newPingOperation(Transport transport) {
      return new PingOperation(codec, topologyId, transport, cacheNameBytes);
   }

   /**
    * Construct a fault tolerant ping request. This operation should be capable
    * to deal with nodes being down, so it will find the first node successful
    * node to respond to the ping.
    *
    * @return a ping operation for the cluster
    */
   public FaultTolerantPingOperation newFaultTolerantPingOperation() {
      return new FaultTolerantPingOperation(
            codec, transportFactory, cacheNameBytes, topologyId, flags());
   }

   private Flag[] flags() {
      List<Flag> flags = this.flagsMap.get();
      this.flagsMap.remove();
      if (forceReturnValue) {
         if (flags == null) {
            return FORCE_RETURN_VALUE;
         } else {
            flags.add(Flag.FORCE_RETURN_VALUE);
         }
      }
      return flags != null ? flags.toArray(new Flag[0]) : null;
   }

   public void setFlags(Flag[] flags) {
      List<Flag> list = new ArrayList<Flag>();
      for(Flag flag : flags)
         list.add(flag);
      this.flagsMap.set(list);
   }

   public void addFlags(Flag... flags) {
      List<Flag> list = this.flagsMap.get();
      if (list == null) {
         list = new ArrayList<Flag>();
         this.flagsMap.set(list);
      }
      for(Flag flag : flags)
         list.add(flag);

   }
}
