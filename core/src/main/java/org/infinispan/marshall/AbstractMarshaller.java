/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.io.ExposedByteArrayOutputStream;

import java.io.IOException;
import java.io.InputStream;

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
      BufferSizePredictor sizePredictor = BufferSizePredictorFactory.getBufferSizePredictor();
      int estimatedSize = sizePredictor.nextSize(obj);
      ByteBuffer byteBuffer = objectToBuffer(obj, estimatedSize);
      int length = byteBuffer.getLength();
      // If the prediction is way off, then trim it
      if (estimatedSize > (length * 4)) {
         byte[] buffer = trimBuffer(byteBuffer);
         byteBuffer = new ByteBuffer(buffer, 0, buffer.length);
      }
      sizePredictor.recordSize(length);
      return byteBuffer;
   }

   @Override
   public byte[] objectToByteBuffer(Object o) throws IOException, InterruptedException {
      BufferSizePredictor sizePredictor = BufferSizePredictorFactory.getBufferSizePredictor();
      byte[] bytes = objectToByteBuffer(o, sizePredictor.nextSize(o));
      sizePredictor.recordSize(bytes.length);
      return bytes;
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      ByteBuffer b = objectToBuffer(obj, estimatedSize);
      return trimBuffer(b);
   }

   private byte[] trimBuffer(ByteBuffer b) {
      byte[] bytes = new byte[b.getLength()];
      System.arraycopy(b.getBuf(), b.getOffset(), bytes, 0, b.getLength());
      return bytes;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return objectFromByteBuffer(buf, 0, buf.length);
   }

   /**
    * This method implements {@link StreamingMarshaller#objectFromInputStream(java.io.InputStream)}, but its
    * implementation has been moved here rather that keeping under a class that implements StreamingMarshaller
    * in order to avoid code duplication.
    */
   public Object objectFromInputStream(InputStream inputStream) throws IOException, ClassNotFoundException {
      int len = inputStream.available();
      ExposedByteArrayOutputStream bytes = new ExposedByteArrayOutputStream(len);
      byte[] buf = new byte[Math.min(len, 1024)];
      int bytesRead;
      while ((bytesRead = inputStream.read(buf, 0, buf.length)) != -1) bytes.write(buf, 0, bytesRead);
      return objectFromByteBuffer(bytes.getRawBuffer(), 0, bytes.size());
   }

}
