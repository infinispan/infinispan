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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements to the stats operation as defined by <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class StatsOperation extends RetryOnFailureOperation {

   public StatsOperation(Codec codec, TransportFactory transportFactory,
            byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, cacheName, topologyId, flags);
   }

   @Override
   protected Transport getTransport(int retryCount) {
      return transportFactory.getTransport();
   }

   @Override
   protected Object executeOperation(Transport transport) {
      Map<String, String> result;
      // 1) write header
      HeaderParams params = writeHeader(transport, STATS_REQUEST);
      transport.flush();

      readHeaderAndValidate(transport, params);
      int nrOfStats = transport.readVInt();

      result = new HashMap<String, String>();
      for (int i = 0; i < nrOfStats; i++) {
         String statName = transport.readString();
         String statValue = transport.readString();
         result.put(statName, statValue);
      }
      return result;
   }
}
