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
package org.infinispan.server.core.transport

import org.jboss.netty.buffer.ChannelBuffer

/**
 * Reads and writes unsigned variable length long values. Even though it's deprecated, do not
 * remove from source code for the moment because it's a good scala example and could be used
 * as reference.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object VLong {

   def write(out: ChannelBuffer, i: Long) {
      if ((i & ~0x7F) == 0) out.writeByte(i.toByte)
      else {
         out.writeByte(((i & 0x7f) | 0x80).toByte)
         write(out, i >>> 7)
      }
   }

   def read(in: ChannelBuffer): Long = {
      val b = in.readByte
      read(in, b, 7, b & 0x7F, 1)
   }

   private def read(in: ChannelBuffer, b: Byte, shift: Int, i: Long, count: Int): Long = {
      if ((b & 0x80) == 0) i
      else {
         if (count > 9)
            throw new IllegalStateException(
               "Stream corrupted.  A variable length long cannot be longer than 9 bytes.")

         val bb = in.readByte
         read(in, bb, shift + 7, i | (bb & 0x7FL) << shift, count + 1)
      }
   }
}