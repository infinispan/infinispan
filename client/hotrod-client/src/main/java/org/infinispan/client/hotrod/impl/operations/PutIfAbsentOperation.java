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
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.util.Util;
import org.infinispan.util.logging.BasicLogFactory;
import org.jboss.logging.BasicLogger;

/**
 * Implements "putIfAbsent" operation as described in  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PutIfAbsentOperation extends AbstractKeyValueOperation {

   private static final BasicLogger log = BasicLogFactory.getLog(PutIfAbsentOperation.class);

   public PutIfAbsentOperation(Codec codec, TransportFactory transportFactory,
                               byte[] key, byte[] cacheName, AtomicInteger topologyId,
                               Flag[] flags, byte[] value, int lifespan, int maxIdle) {
      super(codec, transportFactory, key, cacheName, topologyId, flags, value, lifespan, maxIdle);
   }

   @Override
   protected Object executeOperation(Transport transport) {
      short status = sendPutOperation(transport, PUT_IF_ABSENT_REQUEST, PUT_IF_ABSENT_RESPONSE);
      byte[] previousValue = null;
      if (status == NO_ERROR_STATUS || status == NOT_PUT_REMOVED_REPLACED_STATUS) {
         previousValue = returnPossiblePrevValue(transport);
         if (log.isTraceEnabled()) {
            log.tracef("Returning from putIfAbsent: %s", Util.printArray(previousValue, false));
         }
      }
      return previousValue;
   }
}
