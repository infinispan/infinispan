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

import java.util.concurrent.atomic.AtomicInteger;

import net.jcip.annotations.Immutable;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.BinaryMetadataValue;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * Corresponds to getWithMetadata operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Immutable
public class GetWithMetadataOperation extends AbstractKeyOperation<BinaryMetadataValue> {

   private static final Log log = LogFactory.getLog(GetWithMetadataOperation.class);

   public GetWithMetadataOperation(Codec codec, TransportFactory transportFactory,
            byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, key, cacheName, topologyId, flags);
   }

   @Override
   protected BinaryMetadataValue executeOperation(Transport transport) {
      short status = sendKeyOperation(key, transport, GET_WITH_METADATA, GET_WITH_METADATA_RESPONSE);
      BinaryMetadataValue result = null;
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         result = null;
      } else if (status == NO_ERROR_STATUS) {
         short expiration = transport.readByte();
         long creation = -1;
         int lifespan = -1;
         long lastUsed = -1;
         int maxIdle = -1;
         if ((expiration & 0x01) == 0x01) {
            creation = transport.readLong();
            lifespan = transport.readVInt();
         }
         if ((expiration & 0x02) == 0x02) {
            lastUsed = transport.readLong();
            maxIdle = transport.readVInt();
         }
         long version = transport.readLong();
         if (log.isTraceEnabled()) {
            log.tracef("Received version: %d", version);
         }
         byte[] value = transport.readArray();
         result = new BinaryMetadataValue(creation, lifespan, lastUsed, maxIdle, version, value);
      }
      return result;
   }
}
