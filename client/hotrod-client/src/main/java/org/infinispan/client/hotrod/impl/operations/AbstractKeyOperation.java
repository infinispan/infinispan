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
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic class for all hot rod operations that manipulate a key.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class AbstractKeyOperation extends RetryOnFailureOperation {

   private static final Log log = LogFactory.getLog(AbstractKeyOperation.class);

   protected final byte[] key;

   protected AbstractKeyOperation(TransportFactory transportFactory, byte[] key, byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(transportFactory, cacheName, topologyId, flags);
      this.key = key;
   }

   @Override
   protected Transport getTransport(int retryCount) {
      if (retryCount == 0) {
         return transportFactory.getTransport(key);
      } else {
         return transportFactory.getTransport();
      }
   }

   protected short sendKeyOperation(byte[] key, Transport transport, byte opCode, byte opRespCode) {
      // 1) write [header][key length][key]
      long messageId = writeHeader(transport, opCode);
      transport.writeArray(key);
      transport.flush();

      // 2) now read the header
      return readHeaderAndValidate(transport, messageId, opRespCode);
   }

   protected byte[] returnPossiblePrevValue(Transport transport) {
      if (hasForceReturn(flags)) {
         byte[] bytes = transport.readArray();
         if (log.isTraceEnabled()) log.tracef("Previous value bytes is: %s", Util.printArray(bytes, false));
         //0-length response means null
         return bytes.length == 0 ? null : bytes;
      } else {
         return null;
      }
   }

   private boolean hasForceReturn(Flag[] flags) {
      if (flags == null) return false;
      for (Flag flag : flags) {
         if (flag == Flag.FORCE_RETURN_VALUE) return true;
      }
      return false;
   }

   protected VersionedOperationResponse returnVersionedOperationResponse(Transport transport, long messageId, byte response) {
      //3) ...
      short respStatus = readHeaderAndValidate(transport, messageId, response);

      //4 ...
      VersionedOperationResponse.RspCode code;
      if (respStatus == NO_ERROR_STATUS) {
         code = VersionedOperationResponse.RspCode.SUCCESS;
      } else if (respStatus == NOT_PUT_REMOVED_REPLACED_STATUS) {
         code = VersionedOperationResponse.RspCode.MODIFIED_KEY;
      } else if (respStatus == KEY_DOES_NOT_EXIST_STATUS) {
         code = VersionedOperationResponse.RspCode.NO_SUCH_KEY;
      } else {
         throw new IllegalStateException("Unknown response status: " + Integer.toHexString(respStatus));
      }
      byte[] prevValue = returnPossiblePrevValue(transport);
      return new VersionedOperationResponse(prevValue, code);
   }
}
