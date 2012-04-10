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
package org.infinispan.io;

import java.io.IOException;
import java.io.ObjectInput;

/**
 * A delegating {@link java.io.ObjectInput} that delegates all methods except {@link ObjectInput#close()}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnclosableObjectInputStream implements ObjectInput {
   private final ObjectInput delegate;

   public UnclosableObjectInputStream(ObjectInput delegate) {
      this.delegate = delegate;
   }

   @Override
   public final Object readObject() throws ClassNotFoundException, IOException {
      return delegate.readObject();
   }

   @Override
   public final int read() throws IOException {
      return delegate.read();
   }

   @Override
   public final int read(byte[] b) throws IOException {
      return delegate.read(b);
   }

   @Override
   public final int read(byte[] b, int off, int len) throws IOException {
      return delegate.read(b, off, len);
   }

   @Override
   public final long skip(long n) throws IOException {
      return delegate.skip(n);
   }

   @Override
   public final int available() throws IOException {
      return delegate.available();
   }

   @Override
   public final void close() throws IOException {
      throw new UnsupportedOperationException("close() is not supported in an UnclosableObjectInputStream!");
   }

   @Override
   public final void readFully(byte[] b) throws IOException {
      delegate.readFully(b);
   }

   @Override
   public final void readFully(byte[] b, int off, int len) throws IOException {
      delegate.readFully(b, off, len);
   }

   @Override
   public final int skipBytes(int n) throws IOException {
      return delegate.skipBytes(n);
   }

   @Override
   public final boolean readBoolean() throws IOException {
      return delegate.readBoolean();
   }

   @Override
   public final byte readByte() throws IOException {
      return delegate.readByte();
   }

   @Override
   public final int readUnsignedByte() throws IOException {
      return delegate.readUnsignedByte();
   }

   @Override
   public final short readShort() throws IOException {
      return delegate.readShort();
   }

   @Override
   public final int readUnsignedShort() throws IOException {
      return delegate.readUnsignedShort();
   }

   @Override
   public final char readChar() throws IOException {
      return delegate.readChar();
   }

   @Override
   public final int readInt() throws IOException {
      return delegate.readInt();
   }

   @Override
   public final long readLong() throws IOException {
      return delegate.readLong();
   }

   @Override
   public final float readFloat() throws IOException {
      return delegate.readFloat();
   }

   @Override
   public final double readDouble() throws IOException {
      return delegate.readDouble();
   }

   @Override
   public final String readLine() throws IOException {
      return delegate.readLine();
   }

   @Override
   public final String readUTF() throws IOException {
      return delegate.readUTF();
   }
}
