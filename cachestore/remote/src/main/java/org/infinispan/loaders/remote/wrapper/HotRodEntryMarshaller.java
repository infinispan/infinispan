/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.remote.wrapper;

import java.io.IOException;
import java.util.Arrays;

import org.infinispan.io.ByteBuffer;
import org.infinispan.marshall.BufferSizePredictor;
import org.infinispan.marshall.Marshaller;

/**
 * HotRodEntryMarshaller.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class HotRodEntryMarshaller implements Marshaller {

   BufferSizePredictor predictor = new IdentityBufferSizePredictor();

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      return (byte[]) obj;
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      return (byte[]) obj;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return buf;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return Arrays.copyOfRange(buf, offset, offset+length);
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      byte[] b = (byte[])o;
      return new ByteBuffer(b, 0, b.length);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return o instanceof byte[];
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return predictor;
   }

   class IdentityBufferSizePredictor implements BufferSizePredictor {

      @Override
      public int nextSize(Object obj) {
         return ((byte[])obj).length;
      }

      @Override
      public void recordSize(int previousSize) {
         // NOOP
      }

   }
}
