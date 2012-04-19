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
package org.infinispan.client.hotrod.impl.transport;

import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * Support class for transport implementations.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class AbstractTransport implements Transport {

   private static final Log log = LogFactory.getLog(AbstractTransport.class);

   private final TransportFactory transportFactory;

   protected AbstractTransport(TransportFactory transportFactory) {
      this.transportFactory = transportFactory;
   }

   @Override
   public byte[] readArray() {
      int responseLength = readVInt();
      return readByteArray(responseLength);
   }

   @Override
   public String readString() {
      byte[] strContent = readArray();
      String readString = new String(strContent, HotRodConstants.HOTROD_STRING_CHARSET);
      if (log.isTraceEnabled()) {
         log.tracef("Read string is: %s", readString);
      }
      return readString;
   }

   @Override
   public long readLong() {
      byte[] longBytes = readByteArray(8);
      long result = 0;
      for (byte longByte : longBytes) {
         result <<= 8;
         result ^= (long) longByte & 0xFF;
      }
      return result;
   }

   @Override
   public void writeLong(long longValue) {
      byte[] b = new byte[8];
      for (int i = 0; i < 8; i++) {
         b[7 - i] = (byte) (longValue >>> (i * 8));
      }
      writeBytes(b);
   }

   @Override
   public int readUnsignedShort() {
      byte[] shortBytes = readByteArray(2);
      int result = 0;
      for (byte longByte : shortBytes) {
         result <<= 8;
         result ^= (long) longByte & 0xFF;
      }
      return result;
   }

   @Override
   public int read4ByteInt() {
      byte[] b = readByteArray(4);
      int value = 0;
      for (int i = 0; i < 4; i++) {
         int shift = (4 - 1 - i) * 8;
         value += (b[i] & 0x000000FF) << shift;
      }
      return value;
   }

   @Override
   public void writeString(String string) {
      if (!string.isEmpty()) {
         writeArray(string.getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
      } else {
         writeVInt(0);
      }
   }

   @Override
   public TransportFactory getTransportFactory() {
      return transportFactory;
   }

   @Override
   public void writeArray(byte[] toAppend) {
      writeVInt(toAppend.length);
      writeBytes(toAppend);
   }

   protected abstract void writeBytes(byte[] toAppend);
}
