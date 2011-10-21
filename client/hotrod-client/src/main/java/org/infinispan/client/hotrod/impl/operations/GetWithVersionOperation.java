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
import org.infinispan.client.hotrod.impl.BinaryVersionedValue;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Corresponds to getWithVersion operation as described by
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class GetWithVersionOperation extends AbstractKeyOperation {

   private static final Log log = LogFactory.getLog(GetWithVersionOperation.class);

   public GetWithVersionOperation(Codec codec, TransportFactory transportFactory,
            byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, key, cacheName, topologyId, flags);
   }

   @Override
   protected Object executeOperation(Transport transport) {
      short status = sendKeyOperation(key, transport, GET_WITH_VERSION, GET_WITH_VERSION_RESPONSE);
      Object result = null;
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         result = null;
      } else if (status == NO_ERROR_STATUS) {
         long version = transport.readLong();
         if (log.isTraceEnabled()) {
            log.tracef("Received version: %d", version);
         }
         byte[] value = transport.readArray();
         result = new BinaryVersionedValue(version, value);
      }
      return result;
   }
}
