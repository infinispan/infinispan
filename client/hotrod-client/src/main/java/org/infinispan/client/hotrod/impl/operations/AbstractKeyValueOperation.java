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
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all operations that manipulate a key and a value.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class AbstractKeyValueOperation extends AbstractKeyOperation {

   protected final byte[] value;

   protected final int lifespan;

   protected final int maxIdle;

   protected AbstractKeyValueOperation(Codec codec, TransportFactory transportFactory, byte[] key, byte[] cacheName,
                                       AtomicInteger topologyId, Flag[] flags, byte[] value, int lifespan, int maxIdle) {
      super(codec, transportFactory, key, cacheName, topologyId, flags);
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
   }

   //[header][key length][key][lifespan][max idle][value length][value]
   protected short sendPutOperation(Transport transport, short opCode, byte opRespCode) {
      // 1) write header
      HeaderParams params = writeHeader(transport, opCode);

      // 2) write key and value
      transport.writeArray(key);
      transport.writeVInt(lifespan);
      transport.writeVInt(maxIdle);
      transport.writeArray(value);
      transport.flush();

      // 3) now read header

      //return status (not error status for sure)
      return readHeaderAndValidate(transport, params);
   }
}
