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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import org.infinispan.server.core.transport.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.ChannelBufferIndexFinder;

/**
 * NettyChannelBuffer.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class NettyChannelBuffer implements ChannelBuffer, org.jboss.netty.buffer.ChannelBuffer {
   final org.jboss.netty.buffer.ChannelBuffer buffer;

   public NettyChannelBuffer(org.jboss.netty.buffer.ChannelBuffer buffer) {
      this.buffer = buffer;
   }
   
   @Override
   public byte readByte() {
      return buffer.readByte();
   }

   @Override
   public void readBytes(byte[] dst, int dstIndex, int length) {
      buffer.readBytes(dst, dstIndex, length);
   }

   @Override
   public int compareTo(org.jboss.netty.buffer.ChannelBuffer o) {
      return buffer.compareTo(o);
   }

   @Override
   public int capacity() {
      return buffer.capacity();
   }

   @Override
   public void clear() {
      buffer.clear();
   }

   @Override
   public org.jboss.netty.buffer.ChannelBuffer copy() {
      return buffer.copy();
   }

   @Override
   public org.jboss.netty.buffer.ChannelBuffer copy(int index, int length) {
      return buffer.copy(index, length);
   }

   @Override
   public void discardReadBytes() {
      buffer.discardReadBytes();
   }

   @Override
   public org.jboss.netty.buffer.ChannelBuffer duplicate() {
      return buffer.duplicate();
   }

   @Override
   public ChannelBufferFactory factory() {
      return buffer.factory();
   }

   @Override
   public byte getByte(int index) {
      return buffer.getByte(index);
   }

   @Override
   public void getBytes(int index, org.jboss.netty.buffer.ChannelBuffer dst) {
      buffer.getBytes(index, dst);
   }

   @Override
   public void getBytes(int index, byte[] dst) {
      buffer.getBytes(index, dst);
   }

   @Override
   public void getBytes(int index, ByteBuffer dst) {
      buffer.getBytes(index, dst);
   }

   @Override
   public void getBytes(int index, org.jboss.netty.buffer.ChannelBuffer dst, int length) {
      buffer.getBytes(index, dst, length);
   }

   @Override
   public void getBytes(int index, OutputStream out, int length) throws IOException {
      buffer.getBytes(index, out, length);
   }

   @Override
   public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
      return buffer.getBytes(index, out, length);
   }

   @Override
   public void getBytes(int index, org.jboss.netty.buffer.ChannelBuffer dst, int dstIndex, int length) {
      buffer.getBytes(index, dst, dstIndex, length);
   }

   @Override
   public void getBytes(int index, byte[] dst, int dstIndex, int length) {
      buffer.getBytes(index, dst, dstIndex, length);
   }

   @Override
   public int getInt(int index) {
      return buffer.getInt(index);
   }

   @Override
   public long getLong(int index) {
      return buffer.getLong(index);
   }

   @Override
   public int getMedium(int index) {
      return buffer.getMedium(index);
   }

   @Override
   public short getShort(int index) {
      return buffer.getShort(index);
   }

   @Override
   public short getUnsignedByte(int index) {
      return buffer.getUnsignedByte(index);
   }

   @Override
   public long getUnsignedInt(int index) {
      return buffer.getUnsignedInt(index);
   }

   @Override
   public int getUnsignedMedium(int index) {
      return buffer.getUnsignedMedium(index);
   }

   @Override
   public int getUnsignedShort(int index) {
      return buffer.getUnsignedShort(index);
   }

   @Override
   public int indexOf(int fromIndex, int toIndex, byte value) {
      return buffer.indexOf(fromIndex, toIndex, value);
   }

   @Override
   public int indexOf(int fromIndex, int toIndex, ChannelBufferIndexFinder indexFinder) {
      return buffer.indexOf(fromIndex, toIndex, indexFinder);
   }

   @Override
   public void markReaderIndex() {
      buffer.markReaderIndex();
   }

   @Override
   public void markWriterIndex() {
      buffer.markWriterIndex();
   }

   @Override
   public ByteOrder order() {
      return buffer.order();
   }

   @Override
   public org.jboss.netty.buffer.ChannelBuffer readBytes(int length) {
      return buffer.readBytes(length);
   }

   @Override
   public org.jboss.netty.buffer.ChannelBuffer readBytes(ChannelBufferIndexFinder indexFinder) {
      return buffer.readBytes(indexFinder);
   }

   @Override
   public void readBytes(org.jboss.netty.buffer.ChannelBuffer dst) {
      buffer.readBytes(dst);
   }

   @Override
   public void readBytes(byte[] dst) {
      buffer.readBytes(dst);
   }

   @Override
   public void readBytes(ByteBuffer dst) {
      buffer.readBytes(dst);
   }

   @Override
   public void readBytes(org.jboss.netty.buffer.ChannelBuffer dst, int length) {
      buffer.readBytes(dst, length);
   }

   @Override
   public void readBytes(OutputStream out, int length) throws IOException {
      buffer.readBytes(out, length);
   }

   @Override
   public int readBytes(GatheringByteChannel out, int length) throws IOException {
      return buffer.readBytes(out, length);
   }

   @Override
   public void readBytes(org.jboss.netty.buffer.ChannelBuffer dst, int dstIndex, int length) {
      buffer.readBytes(dst, dstIndex, length);
   }

   @Override
   public int readInt() {
      return buffer.readInt();
   }

   @Override
   public long readLong() {
      return buffer.readLong();
   }

   @Override
   public int readMedium() {
      return buffer.readMedium();
   }

   @Override
   public short readShort() {
      return buffer.readShort();
   }

   @Override
   public org.jboss.netty.buffer.ChannelBuffer readSlice(int length) {
      return buffer.readSlice(length);
   }

   @Override
   public org.jboss.netty.buffer.ChannelBuffer readSlice(ChannelBufferIndexFinder indexFinder) {
      return buffer.readSlice(indexFinder);
   }

   @Override
   public short readUnsignedByte() {
      return buffer.readUnsignedByte();
   }

   @Override
   public long readUnsignedInt() {
      return buffer.readUnsignedInt();
   }

   @Override
   public int readUnsignedMedium() {
      return buffer.readUnsignedMedium();
   }

   @Override
   public int readUnsignedShort() {
      return buffer.readUnsignedShort();
   }

   @Override
   public boolean readable() {
      return buffer.readable();
   }

   @Override
   public int readableBytes() {
      return buffer.readableBytes();
   }

   @Override
   public int readerIndex() {
      return buffer.readerIndex();
   }

   @Override
   public void readerIndex(int readerIndex) {
      buffer.readerIndex(readerIndex);
   }

   @Override
   public void resetReaderIndex() {
      buffer.resetReaderIndex();
   }

   @Override
   public void resetWriterIndex() {
      buffer.resetWriterIndex();
   }

   @Override
   public void setByte(int index, byte value) {
      buffer.setByte(index, value);
   }

   @Override
   public void setBytes(int index, org.jboss.netty.buffer.ChannelBuffer src) {
      buffer.setBytes(index, src);
   }

   @Override
   public void setBytes(int index, byte[] src) {
      buffer.setBytes(index, src);
   }

   @Override
   public void setBytes(int index, ByteBuffer src) {
      buffer.setBytes(index, src);
   }

   @Override
   public void setBytes(int index, org.jboss.netty.buffer.ChannelBuffer src, int length) {
      buffer.setBytes(index, src, length);
   }

   @Override
   public int setBytes(int index, InputStream in, int length) throws IOException {
      return buffer.setBytes(index, in, length);
   }

   @Override
   public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
      return buffer.setBytes(index, in, length);
   }

   @Override
   public void setBytes(int index, org.jboss.netty.buffer.ChannelBuffer src, int srcIndex, int length) {
      buffer.setBytes(index, src, srcIndex, length);
   }

   @Override
   public void setBytes(int index, byte[] src, int srcIndex, int length) {
      buffer.setBytes(index, src, srcIndex, length);
   }

   @Override
   public void setIndex(int readerIndex, int writerIndex) {
      buffer.setIndex(readerIndex, writerIndex);
   }

   @Override
   public void setInt(int index, int value) {
      buffer.setInt(index, value);
   }

   @Override
   public void setLong(int index, long value) {
      buffer.setLong(index, value);
   }

   @Override
   public void setMedium(int index, int value) {
      buffer.setMedium(index, value);
   }

   @Override
   public void setShort(int index, short value) {
      buffer.setShort(index, value);
   }

   @Override
   public void setZero(int index, int length) {
      buffer.setZero(index, length);
   }

   @Override
   public void skipBytes(int length) {
      buffer.skipBytes(length);
   }

   @Override
   public int skipBytes(ChannelBufferIndexFinder indexFinder) {
      return buffer.skipBytes(indexFinder);
   }

   @Override
   public org.jboss.netty.buffer.ChannelBuffer slice() {
      return buffer.slice();
   }

   @Override
   public org.jboss.netty.buffer.ChannelBuffer slice(int index, int length) {
      return buffer.slice(index, length);
   }

   @Override
   public ByteBuffer toByteBuffer() {
      return buffer.toByteBuffer();
   }

   @Override
   public ByteBuffer toByteBuffer(int index, int length) {
      return buffer.toByteBuffer(index, length);
   }

   @Override
   public ByteBuffer[] toByteBuffers() {
      return buffer.toByteBuffers();
   }

   @Override
   public ByteBuffer[] toByteBuffers(int index, int length) {
      return buffer.toByteBuffers(index, length);
   }

   @Override
   public String toString(String charsetName) {
      return buffer.toString(charsetName);
   }

   @Override
   public String toString(String charsetName, ChannelBufferIndexFinder terminatorFinder) {
      return buffer.toString(charsetName, terminatorFinder);
   }

   @Override
   public String toString(int index, int length, String charsetName) {
      return buffer.toString(index, length, charsetName);
   }

   @Override
   public String toString(int index, int length, String charsetName, ChannelBufferIndexFinder terminatorFinder) {
      return buffer.toString(charsetName, terminatorFinder);
   }

   @Override
   public boolean writable() {
      return buffer.writable();
   }

   @Override
   public int writableBytes() {
      return buffer.writableBytes();
   }

   @Override
   public void writeByte(byte value) {
      buffer.writeByte(value);
   }

   @Override
   public void writeBytes(org.jboss.netty.buffer.ChannelBuffer src) {
      buffer.writeBytes(src);
   }

   @Override
   public void writeBytes(byte[] src) {
      buffer.writeBytes(src);
   }

   @Override
   public void writeBytes(ByteBuffer src) {
      buffer.writeBytes(src);
   }

   @Override
   public void writeBytes(org.jboss.netty.buffer.ChannelBuffer src, int length) {
      buffer.writeBytes(src, length);
   }

   @Override
   public int writeBytes(InputStream in, int length) throws IOException {
      return buffer.writeBytes(in, length);
   }

   @Override
   public int writeBytes(ScatteringByteChannel in, int length) throws IOException {
      return buffer.writeBytes(in, length);
   }

   @Override
   public void writeBytes(org.jboss.netty.buffer.ChannelBuffer src, int srcIndex, int length) {
      buffer.writeBytes(src, srcIndex, length);
   }

   @Override
   public void writeBytes(byte[] src, int srcIndex, int length) {
      buffer.writeBytes(src, srcIndex, length);
   }

   @Override
   public void writeInt(int value) {
      buffer.writeInt(value);
   }

   @Override
   public void writeLong(long value) {
      buffer.writeLong(value);
   }

   @Override
   public void writeMedium(int value) {
      buffer.writeMedium(value);
   }

   @Override
   public void writeShort(short value) {
      buffer.writeShort(value);
   }

   @Override
   public void writeZero(int length) {
      buffer.writeZero(length);
   }

   @Override
   public int writerIndex() {
      return buffer.writerIndex();
   }

   @Override
   public void writerIndex(int writerIndex) {
      buffer.writerIndex(writerIndex);
   }

}
