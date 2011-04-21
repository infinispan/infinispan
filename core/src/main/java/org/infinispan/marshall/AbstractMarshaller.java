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
package org.infinispan.marshall;

import org.infinispan.io.ByteBuffer;

import java.io.IOException;

/**
 * Abstract Marshaller implementation containing shared implementations.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public abstract class AbstractMarshaller implements Marshaller {

   protected static final int DEFAULT_BUF_SIZE = 512;

   /**
    * This is a convenience method for converting an object into a {@link org.infinispan.io.ByteBuffer} which takes
    * an estimated size as parameter. A {@link org.infinispan.io.ByteBuffer} allows direct access to the byte
    * array with minimal array copying
    *
    * @param o object to marshall
    * @param estimatedSize an estimate of how large the resulting byte array may be
    * @return a ByteBuffer
    * @throws Exception
    */
   protected abstract ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException;

   @Override
   public ByteBuffer objectToBuffer(Object obj) throws IOException, InterruptedException {
      return objectToBuffer(obj, DEFAULT_BUF_SIZE);
   }

   @Override
   public byte[] objectToByteBuffer(Object o) throws IOException, InterruptedException {
      return objectToByteBuffer(o, DEFAULT_BUF_SIZE);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      ByteBuffer b = objectToBuffer(obj, estimatedSize);
      byte[] bytes = new byte[b.getLength()];
      System.arraycopy(b.getBuf(), b.getOffset(), bytes, 0, b.getLength());
      return bytes;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return objectFromByteBuffer(buf, 0, buf.length);
   }

}
