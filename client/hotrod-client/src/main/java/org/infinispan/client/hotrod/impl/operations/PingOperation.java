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

   public PingOperation(Flag[] flags, AtomicInteger topologyId, Transport transport) {
      super(flags, DEFAULT_CACHE_NAME_BYTES, topologyId);
      this.transport = transport;
   }

   @Override
   public Object execute() {
      boolean success;
      try {
         long messageId = writeHeader(transport, HotRodConstants.PING_REQUEST);
         transport.flush();

         short respStatus = readHeaderAndValidate(transport, messageId, HotRodConstants.PING_RESPONSE);
         if (respStatus == HotRodConstants.NO_ERROR_STATUS) {
            if (log.isTraceEnabled())
               log.trace("Successfully validated transport: " + transport);
            success = true;
         } else {
            if (log.isTraceEnabled())
               log.trace("Unknown response status: " + respStatus);
            success = false;
         }
      } catch (Exception e) {
         if (log.isTraceEnabled())
            log.trace("Failed to validate transport: " + transport, e);
         success = false;
      }
      return success;
   }
}
