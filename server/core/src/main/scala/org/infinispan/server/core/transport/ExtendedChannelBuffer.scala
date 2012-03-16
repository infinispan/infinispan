/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

object ExtendedChannelBuffer {

   def wrappedBuffer(array: Array[Byte]*) = ChannelBuffers.wrappedBuffer(array : _*)
   def dynamicBuffer = ChannelBuffers.dynamicBuffer()

   def readUnsignedShort(bf: ChannelBuffer): Int = bf.readUnsignedShort
   def readUnsignedInt(bf: ChannelBuffer): Int = VInt.read(bf)
   def readUnsignedLong(bf: ChannelBuffer): Long = VLong.read(bf)

   def readRangedBytes(bf: ChannelBuffer): Array[Byte] = {
      val length = readUnsignedInt(bf)
      if (length > 0) {
         val array = new Array[Byte](length)
         bf.readBytes(array)
         array;
      } else {
         Array[Byte]()
      }
   }

   /**
    * Reads length of String and then returns an UTF-8 formatted String of such length.
    * If the length is 0, an empty String is returned.
    */
   def readString(bf: ChannelBuffer): String = {
      val bytes = readRangedBytes(bf)
      if (!bytes.isEmpty) new String(bytes, "UTF8") else ""
   }

   def writeUnsignedShort(i: Int, bf: ChannelBuffer) = bf.writeShort(i)
   def writeUnsignedInt(i: Int, bf: ChannelBuffer) = VInt.write(bf, i)
   def writeUnsignedLong(l: Long, bf: ChannelBuffer) = VLong.write(bf, l)

   def writeRangedBytes(src: Array[Byte], bf: ChannelBuffer) {
      writeUnsignedInt(src.length, bf)
      bf.writeBytes(src)
   }

   def writeString(msg: String, bf: ChannelBuffer) = writeRangedBytes(msg.getBytes(), bf)

}