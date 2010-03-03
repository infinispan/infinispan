/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.server.core.transport.netty;

import org.infinispan.server.core.transport.ChannelBuffer;

/**
 * NettyChannelBuffer.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
public class NettyChannelBuffer implements ChannelBuffer /*, org.jboss.netty.nettyBuffer.ChannelBuffer*/ {
   private final org.jboss.netty.buffer.ChannelBuffer nettyBuffer;

   public NettyChannelBuffer(org.jboss.netty.buffer.ChannelBuffer nettyBuffer) {
      this.nettyBuffer = nettyBuffer;
   }

   @Override
   public org.jboss.netty.buffer.ChannelBuffer getUnderlyingChannelBuffer() {
      return nettyBuffer;
   }

   @Override
   public int readUnsignedInt() {
      byte b = readByte();
      int i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   @Override
   public void writeUnsignedInt(int i) {
      while ((i & ~0x7F) != 0) {
         writeByte((byte) ((i & 0x7f) | 0x80));
         i >>>= 7;
      }
      writeByte((byte) i);
   }

   @Override
   public long readUnsignedLong() {
      byte b = readByte();
      long l = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = readByte();
         l |= (b & 0x7FL) << shift;
      }
      return l;
   }

   @Override
   public void writeUnsignedLong(long l) {
      while ((l & ~0x7F) != 0) {
         writeByte((byte) ((l & 0x7f) | 0x80));
         l >>>= 7;
      }
      writeByte((byte) l);
   }

   @Override
   public byte readByte() {
      return nettyBuffer.readByte();
   }

   @Override
   public void readBytes(byte[] dst, int dstIndex, int length) {
      nettyBuffer.readBytes(dst, dstIndex, length);
   }

//   @Override
//   public int compareTo(org.jboss.netty.nettyBuffer.ChannelBuffer o) {
//      return nettyBuffer.compareTo(o);
//   }
//
//   @Override
//   public int capacity() {
//      return nettyBuffer.capacity();
//   }
//
//   @Override
//   public void clear() {
//      nettyBuffer.clear();
//   }
//
//   @Override
//   public org.jboss.netty.nettyBuffer.ChannelBuffer copy() {
//      return nettyBuffer.copy();
//   }
//
//   @Override
//   public org.jboss.netty.nettyBuffer.ChannelBuffer copy(int index, int length) {
//      return nettyBuffer.copy(index, length);
//   }
//
//   @Override
//   public void discardReadBytes() {
//      nettyBuffer.discardReadBytes();
//   }
//
//   @Override
//   public org.jboss.netty.nettyBuffer.ChannelBuffer duplicate() {
//      return nettyBuffer.duplicate();
//   }
//
//   @Override
//   public ChannelBufferFactory factory() {
//      return nettyBuffer.factory();
//   }
//
//   @Override
//   public byte getByte(int index) {
//      return nettyBuffer.getByte(index);
//   }
//
//   @Override
//   public void getBytes(int index, org.jboss.netty.nettyBuffer.ChannelBuffer dst) {
//      nettyBuffer.getBytes(index, dst);
//   }
//
//   @Override
//   public void getBytes(int index, byte[] dst) {
//      nettyBuffer.getBytes(index, dst);
//   }
//
//   @Override
//   public void getBytes(int index, ByteBuffer dst) {
//      nettyBuffer.getBytes(index, dst);
//   }
//
//   @Override
//   public void getBytes(int index, org.jboss.netty.nettyBuffer.ChannelBuffer dst, int length) {
//      nettyBuffer.getBytes(index, dst, length);
//   }
//
//   @Override
//   public void getBytes(int index, OutputStream out, int length) throws IOException {
//      nettyBuffer.getBytes(index, out, length);
//   }
//
//   @Override
//   public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
//      return nettyBuffer.getBytes(index, out, length);
//   }
//
//   @Override
//   public void getBytes(int index, org.jboss.netty.nettyBuffer.ChannelBuffer dst, int dstIndex, int length) {
//      nettyBuffer.getBytes(index, dst, dstIndex, length);
//   }
//
//   @Override
//   public void getBytes(int index, byte[] dst, int dstIndex, int length) {
//      nettyBuffer.getBytes(index, dst, dstIndex, length);
//   }
//
//   @Override
//   public int getInt(int index) {
//      return nettyBuffer.getInt(index);
//   }
//
//   @Override
//   public long getLong(int index) {
//      return nettyBuffer.getLong(index);
//   }
//
//   @Override
//   public int getMedium(int index) {
//      return nettyBuffer.getMedium(index);
//   }
//
//   @Override
//   public short getShort(int index) {
//      return nettyBuffer.getShort(index);
//   }
//
//   @Override
//   public short getUnsignedByte(int index) {
//      return nettyBuffer.getUnsignedByte(index);
//   }
//
//   @Override
//   public long getUnsignedInt(int index) {
//      return nettyBuffer.getUnsignedInt(index);
//   }
//
//   @Override
//   public int getUnsignedMedium(int index) {
//      return nettyBuffer.getUnsignedMedium(index);
//   }
//
//   @Override
//   public int getUnsignedShort(int index) {
//      return nettyBuffer.getUnsignedShort(index);
//   }
//
//   @Override
//   public int indexOf(int fromIndex, int toIndex, byte value) {
//      return nettyBuffer.indexOf(fromIndex, toIndex, value);
//   }
//
//   @Override
//   public int indexOf(int fromIndex, int toIndex, ChannelBufferIndexFinder indexFinder) {
//      return nettyBuffer.indexOf(fromIndex, toIndex, indexFinder);
//   }
//
//   @Override
//   public void markReaderIndex() {
//      nettyBuffer.markReaderIndex();
//   }
//
//   @Override
//   public void markWriterIndex() {
//      nettyBuffer.markWriterIndex();
//   }
//
//   @Override
//   public ByteOrder order() {
//      return nettyBuffer.order();
//   }
//
     @Override
     public ChannelBuffer readBytes(int length) {
        return new NettyChannelBuffer(nettyBuffer.readBytes(length));
     }

//
//   @Override
//   public org.jboss.netty.nettyBuffer.ChannelBuffer readBytes(ChannelBufferIndexFinder indexFinder) {
//      return nettyBuffer.readBytes(indexFinder);
//   }
//
//   @Override
//   public void readBytes(org.jboss.netty.nettyBuffer.ChannelBuffer dst) {
//      nettyBuffer.readBytes(dst);
//   }
//
   @Override
   public void readBytes(byte[] dst) {
      nettyBuffer.readBytes(dst);
   }
   
//
//   @Override
//   public void readBytes(ByteBuffer dst) {
//      nettyBuffer.readBytes(dst);
//   }
//
//   @Override
//   public void readBytes(org.jboss.netty.nettyBuffer.ChannelBuffer dst, int length) {
//      nettyBuffer.readBytes(dst, length);
//   }
//
//   @Override
//   public void readBytes(OutputStream out, int length) throws IOException {
//      nettyBuffer.readBytes(out, length);
//   }
//
//   @Override
//   public int readBytes(GatheringByteChannel out, int length) throws IOException {
//      return nettyBuffer.readBytes(out, length);
//   }
//
//   @Override
//   public void readBytes(org.jboss.netty.nettyBuffer.ChannelBuffer dst, int dstIndex, int length) {
//      nettyBuffer.readBytes(dst, dstIndex, length);
//   }
//
//   @Override
//   public int readInt() {
//      return nettyBuffer.readInt();
//   }
//
//   @Override
//   public long readLong() {
//      return nettyBuffer.readLong();
//   }
//
//   @Override
//   public int readMedium() {
//      return nettyBuffer.readMedium();
//   }
//
//   @Override
//   public short readShort() {
//      return nettyBuffer.readShort();
//   }
//
//   @Override
//   public org.jboss.netty.nettyBuffer.ChannelBuffer readSlice(int length) {
//      return nettyBuffer.readSlice(length);
//   }
//
//   @Override
//   public org.jboss.netty.nettyBuffer.ChannelBuffer readSlice(ChannelBufferIndexFinder indexFinder) {
//      return nettyBuffer.readSlice(indexFinder);
//   }
//
     @Override
     public short readUnsignedByte() {
        return nettyBuffer.readUnsignedByte();
     }

//
//   @Override
//   public long readUnsignedInt() {
//      return nettyBuffer.readUnsignedInt();
//   }
//
//   @Override
//   public int readUnsignedMedium() {
//      return nettyBuffer.readUnsignedMedium();
//   }
//
//   @Override
//   public int readUnsignedShort() {
//      return nettyBuffer.readUnsignedShort();
//   }
//
//   @Override
//   public boolean readable() {
//      return nettyBuffer.readable();
//   }
//
//     @Override
//     public int readableBytes() {
//        return nettyBuffer.readableBytes();
//     }
//
   @Override
   public int readerIndex() {
      return nettyBuffer.readerIndex();
   }

//
//
//   @Override
//   public void readerIndex(int readerIndex) {
//      nettyBuffer.readerIndex(readerIndex);
//   }
//
//     @Override
//     public void resetReaderIndex() {
//        nettyBuffer.resetReaderIndex();
//     }
//
//   @Override
//   public void resetWriterIndex() {
//      nettyBuffer.resetWriterIndex();
//   }
//
//   @Override
//   public void setByte(int index, byte value) {
//      nettyBuffer.setByte(index, value);
//   }
//
//   @Override
//   public void setBytes(int index, org.jboss.netty.nettyBuffer.ChannelBuffer src) {
//      nettyBuffer.setBytes(index, src);
//   }
//
//   @Override
//   public void setBytes(int index, byte[] src) {
//      nettyBuffer.setBytes(index, src);
//   }
//
//   @Override
//   public void setBytes(int index, ByteBuffer src) {
//      nettyBuffer.setBytes(index, src);
//   }
//
//   @Override
//   public void setBytes(int index, org.jboss.netty.nettyBuffer.ChannelBuffer src, int length) {
//      nettyBuffer.setBytes(index, src, length);
//   }
//
//   @Override
//   public int setBytes(int index, InputStream in, int length) throws IOException {
//      return nettyBuffer.setBytes(index, in, length);
//   }
//
//   @Override
//   public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
//      return nettyBuffer.setBytes(index, in, length);
//   }
//
//   @Override
//   public void setBytes(int index, org.jboss.netty.nettyBuffer.ChannelBuffer src, int srcIndex, int length) {
//      nettyBuffer.setBytes(index, src, srcIndex, length);
//   }
//
//   @Override
//   public void setBytes(int index, byte[] src, int srcIndex, int length) {
//      nettyBuffer.setBytes(index, src, srcIndex, length);
//   }
//
//   @Override
//   public void setIndex(int readerIndex, int writerIndex) {
//      nettyBuffer.setIndex(readerIndex, writerIndex);
//   }
//
//   @Override
//   public void setInt(int index, int value) {
//      nettyBuffer.setInt(index, value);
//   }
//
//   @Override
//   public void setLong(int index, long value) {
//      nettyBuffer.setLong(index, value);
//   }
//
//   @Override
//   public void setMedium(int index, int value) {
//      nettyBuffer.setMedium(index, value);
//   }
//
//   @Override
//   public void setShort(int index, short value) {
//      nettyBuffer.setShort(index, value);
//   }
//
//   @Override
//   public void setZero(int index, int length) {
//      nettyBuffer.setZero(index, length);
//   }
//
//   @Override
//   public void skipBytes(int length) {
//      nettyBuffer.skipBytes(length);
//   }
//
//   @Override
//   public int skipBytes(ChannelBufferIndexFinder indexFinder) {
//      return nettyBuffer.skipBytes(indexFinder);
//   }
//
//   @Override
//   public org.jboss.netty.nettyBuffer.ChannelBuffer slice() {
//      return nettyBuffer.slice();
//   }
//
//   @Override
//   public org.jboss.netty.nettyBuffer.ChannelBuffer slice(int index, int length) {
//      return nettyBuffer.slice(index, length);
//   }
//
//   @Override
//   public ByteBuffer toByteBuffer() {
//      return nettyBuffer.toByteBuffer();
//   }
//
//   @Override
//   public ByteBuffer toByteBuffer(int index, int length) {
//      return nettyBuffer.toByteBuffer(index, length);
//   }
//
//   @Override
//   public ByteBuffer[] toByteBuffers() {
//      return nettyBuffer.toByteBuffers();
//   }
//
//   @Override
//   public ByteBuffer[] toByteBuffers(int index, int length) {
//      return nettyBuffer.toByteBuffers(index, length);
//   }
//
//   @Override
//   public String toString(String charsetName) {
//      return nettyBuffer.toString(charsetName);
//   }
//
//   @Override
//   public String toString(String charsetName, ChannelBufferIndexFinder terminatorFinder) {
//      return nettyBuffer.toString(charsetName, terminatorFinder);
//   }
//
//   @Override
//   public String toString(int index, int length, String charsetName) {
//      return nettyBuffer.toString(index, length, charsetName);
//   }
//
//   @Override
//   public String toString(int index, int length, String charsetName, ChannelBufferIndexFinder terminatorFinder) {
//      return nettyBuffer.toString(charsetName, terminatorFinder);
//   }
//
//   @Override
//   public boolean writable() {
//      return nettyBuffer.writable();
//   }
//
//   @Override
//   public int writableBytes() {
//      return nettyBuffer.writableBytes();
//   }
//
     @Override
     public void writeByte(byte value) {
        nettyBuffer.writeByte(value);
     }

//
//   @Override
//   public void writeBytes(org.jboss.netty.nettyBuffer.ChannelBuffer src) {
//      nettyBuffer.writeBytes(src);
//   }
//
     @Override
     public void writeBytes(byte[] src) {
        nettyBuffer.writeBytes(src);
     }

//
//   @Override
//   public void writeBytes(ByteBuffer src) {
//      nettyBuffer.writeBytes(src);
//   }
//
//   @Override
//   public void writeBytes(org.jboss.netty.nettyBuffer.ChannelBuffer src, int length) {
//      nettyBuffer.writeBytes(src, length);
//   }
//
//   @Override
//   public int writeBytes(InputStream in, int length) throws IOException {
//      return nettyBuffer.writeBytes(in, length);
//   }
//
//   @Override
//   public int writeBytes(ScatteringByteChannel in, int length) throws IOException {
//      return nettyBuffer.writeBytes(in, length);
//   }
//
//   @Override
//   public void writeBytes(org.jboss.netty.nettyBuffer.ChannelBuffer src, int srcIndex, int length) {
//      nettyBuffer.writeBytes(src, srcIndex, length);
//   }
//
//   @Override
//   public void writeBytes(byte[] src, int srcIndex, int length) {
//      nettyBuffer.writeBytes(src, srcIndex, length);
//   }
//
//   @Override
//   public void writeInt(int value) {
//      nettyBuffer.writeInt(value);
//   }
//
//   @Override
//   public void writeLong(long value) {
//      nettyBuffer.writeLong(value);
//   }
//
//   @Override
//   public void writeMedium(int value) {
//      nettyBuffer.writeMedium(value);
//   }
//
//   @Override
//   public void writeShort(short value) {
//      nettyBuffer.writeShort(value);
//   }
//
//   @Override
//   public void writeZero(int length) {
//      nettyBuffer.writeZero(length);
//   }
//
   @Override
   public int writerIndex() {
      return nettyBuffer.writerIndex();
   }
//
//   @Override
//   public void writerIndex(int writerIndex) {
//      nettyBuffer.writerIndex(writerIndex);
//   }

}
