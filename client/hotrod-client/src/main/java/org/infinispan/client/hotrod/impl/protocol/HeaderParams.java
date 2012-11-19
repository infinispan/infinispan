/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.Flag;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hot Rod request header parameters
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class HeaderParams {

   short opCode;
   short opRespCode;
   byte[] cacheName;
   Flag[] flags;
   byte clientIntel;
   byte txMarker;
   AtomicInteger topologyId;
   long messageId;

   public HeaderParams opCode(short opCode) {
      this.opCode = opCode;
      this.opRespCode = toOpRespCode(opCode);
      return this;
   }

   public HeaderParams cacheName(byte[] cacheName) {
      this.cacheName = cacheName;
      return this;
   }

   public HeaderParams flags(Flag[] flags) {
      this.flags = flags;
      return this;
   }

   public HeaderParams clientIntel(byte clientIntel) {
      this.clientIntel = clientIntel;
      return this;
   }

   public HeaderParams txMarker(byte txMarker) {
      this.txMarker = txMarker;
      return this;
   }

   public HeaderParams topologyId(AtomicInteger topologyId) {
      this.topologyId = topologyId;
      return this;
   }

   public HeaderParams messageId(long messageId) {
      this.messageId = messageId;
      return this;
   }

   private short toOpRespCode(short opCode) {
      switch (opCode) {
         case HotRodConstants.PUT_REQUEST:
            return HotRodConstants.PUT_RESPONSE;
         case HotRodConstants.GET_REQUEST:
            return HotRodConstants.GET_RESPONSE;
         case HotRodConstants.PUT_IF_ABSENT_REQUEST:
            return HotRodConstants.PUT_IF_ABSENT_RESPONSE;
         case HotRodConstants.REPLACE_REQUEST:
            return HotRodConstants.REPLACE_RESPONSE;
         case HotRodConstants.REPLACE_IF_UNMODIFIED_REQUEST:
            return HotRodConstants.REPLACE_IF_UNMODIFIED_RESPONSE;
         case HotRodConstants.REMOVE_REQUEST:
            return HotRodConstants.REMOVE_RESPONSE;
         case HotRodConstants.REMOVE_IF_UNMODIFIED_REQUEST:
            return HotRodConstants.REMOVE_IF_UNMODIFIED_RESPONSE;
         case HotRodConstants.CONTAINS_KEY_REQUEST:
            return HotRodConstants.CONTAINS_KEY_RESPONSE;
         case HotRodConstants.GET_WITH_VERSION:
            return HotRodConstants.GET_WITH_VERSION_RESPONSE;
         case HotRodConstants.CLEAR_REQUEST:
            return HotRodConstants.CLEAR_RESPONSE;
         case HotRodConstants.STATS_REQUEST:
            return HotRodConstants.STATS_RESPONSE;
         case HotRodConstants.PING_REQUEST:
            return HotRodConstants.PING_RESPONSE;
         case HotRodConstants.BULK_GET_REQUEST:
            return HotRodConstants.BULK_GET_RESPONSE;
         case HotRodConstants.GET_WITH_METADATA:
            return HotRodConstants.GET_WITH_METADATA_RESPONSE;
         default:
            throw new IllegalStateException("Unknown operation code: " + opCode);
      }
   }



}
