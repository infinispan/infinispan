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

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reads more keys at a time. Specified <a href="http://community.jboss.org/wiki/HotRodBulkGet-Design">here</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class BulkGetOperation extends RetryOnFailureOperation {

   private final int entryCount;

   public BulkGetOperation(TransportFactory transportFactory, byte[] cacheName, AtomicInteger topologyId, Flag[] flags, int entryCount) {
      super(transportFactory, cacheName, topologyId, flags);
      this.entryCount = entryCount;
   }
   
   @Override
   protected Transport getTransport(int retryCount) {
      return transportFactory.getTransport();
   }

   @Override
   protected Object executeOperation(Transport transport) {
      long messageId = writeHeader(transport, BULK_GET_REQUEST);
      transport.writeVInt(entryCount);
      transport.flush();
      readHeaderAndValidate(transport, messageId, BULK_GET_RESPONSE);
      HashMap result = new HashMap();
      while ( transport.readByte() == 1) { //there's more!
         result.put(transport.readArray(), transport.readArray());
      }
      return result;
   }
}
