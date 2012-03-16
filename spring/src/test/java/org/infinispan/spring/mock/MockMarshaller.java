/**
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
 *   ~
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

package org.infinispan.spring.mock;

import java.io.IOException;

import org.infinispan.io.ByteBuffer;
import org.infinispan.marshall.Marshaller;

public final class MockMarshaller implements Marshaller {

   @Override
   public byte[] objectToByteBuffer(final Object obj, final int estimatedSize) throws IOException,
            InterruptedException {
      return null;
   }

   @Override
   public byte[] objectToByteBuffer(final Object obj) throws IOException, InterruptedException {
      return null;
   }

   @Override
   public Object objectFromByteBuffer(final byte[] buf) throws IOException, ClassNotFoundException {
      return null;
   }

   @Override
   public Object objectFromByteBuffer(final byte[] buf, final int offset, final int length)
            throws IOException, ClassNotFoundException {
      return null;
   }

   @Override
   public ByteBuffer objectToBuffer(final Object o) throws IOException, InterruptedException {
      return null;
   }

   @Override
   public boolean isMarshallable(final Object o) {
      return false;
   }
}
