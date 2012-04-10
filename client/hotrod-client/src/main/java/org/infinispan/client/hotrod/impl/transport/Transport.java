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

/**
 * Transport abstraction.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface Transport {

   TransportFactory getTransportFactory();

   void writeArray(byte[] toAppend);

   void writeByte(short toWrite);

   void writeVInt(int vint);

   void writeVLong(long l);

   long readVLong();

   int readVInt();

   void flush();

   short readByte();

   void release();

   /**
    * reads an vint which is size; then an array having that size.
    */
   byte[] readArray();

   String readString();

   byte[] readByteArray(int size);

   long readLong();

   void writeLong(long longValue);

   int readUnsignedShort();

   int read4ByteInt();

   void writeString(String string);

   byte[] dumpStream();
}
