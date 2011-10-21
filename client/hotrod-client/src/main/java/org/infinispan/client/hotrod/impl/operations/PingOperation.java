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
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Corresponds to the "ping" operation as defined in <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public class PingOperation extends HotRodOperation {

   private static final Log log = LogFactory.getLog(PingOperation.class);

   private final Transport transport;

   public PingOperation(Codec codec, AtomicInteger topologyId, Transport transport) {
      this(codec, topologyId, transport, DEFAULT_CACHE_NAME_BYTES);
   }

   public PingOperation(Codec codec, AtomicInteger topologyId, Transport transport, byte[] cacheName) {
      super(codec, null, cacheName, topologyId);
      this.transport = transport;
   }

   @Override
   public PingResult execute() {
      try {
         HeaderParams params = writeHeader(transport, HotRodConstants.PING_REQUEST);
         transport.flush();

         short respStatus = readHeaderAndValidate(transport, params);
         if (respStatus == HotRodConstants.NO_ERROR_STATUS) {
            if (log.isTraceEnabled())
               log.tracef("Successfully validated transport: %s", transport);
            return PingResult.SUCCESS;
         } else {
            if (log.isTraceEnabled())
               log.tracef("Unknown response status: %s", respStatus);
            return PingResult.FAIL;
         }
      } catch (HotRodClientException e) {
         if (e.getMessage().contains("CacheNotFoundException"))
            return PingResult.CACHE_DOES_NOT_EXIST;
         else
            return PingResult.FAIL;
      } catch (Exception e) {
         if (log.isTraceEnabled())
            log.tracef(e, "Failed to validate transport: %s", transport);
         return PingResult.FAIL;
      }
   }

   public static enum PingResult {
      // Success if the ping request was responded correctly
      SUCCESS,
      // When the ping request fails due to non-existing cache
      CACHE_DOES_NOT_EXIST,
      // For any other type of failures
      FAIL,
   }
}
