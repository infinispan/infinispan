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

package org.infinispan.loaders.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.util.Util;

/**
 * This class defines the files where cache data is stored
 * 
 * Insert Operation format
 * flag|datalength|keyHash|ExpiryTime|keyLength|Key|valueLength|Value
 * 
 * Delete Operation format
 * flag|dataLength|keyHash|0L
 * 
 * @author Patrick Azogni
 */
public class FileStore implements Comparable<Object> {

   public static final int READ_ONLY = 1;
   public static final int READ_WRITE = 2;
   private static final byte[] MAGIC = new byte[] { 'F', 'C', 'S', '1' };
   private static final int MODE_POS = MAGIC.length;
   public static final int KEY_LEN_POS = 20;
   public static final int HEADER_LENGTH = 8;
   public static final int MINIMUM_LENGTH = 20;
   public static final int MINIMUM_LENGTH_INSERT = MINIMUM_LENGTH + 8;

   public static final int DELETE_FLAG = 2;
   public static final int INSERT_FLAG = 1;

   private FileChannel channel;
   private RandomAccessFile file;
   private final String filename;
   private final long maxSize; 
   private volatile int mode;

   private volatile AtomicInteger dataSize =  new AtomicInteger(0);
   private AtomicInteger readers = new AtomicInteger(0);

   public FileStore (String filename, String location, long maxSize) throws Exception {

      this.filename = filename;
      this.maxSize = maxSize;
      File f = new File(location, filename);
      file = new RandomAccessFile(f, "rw");
      channel = file.getChannel();
      channel.position(channel.size());

   }
   
   public AtomicInteger getDataSize() {
      return dataSize;
   }
   
   /**
    * Return the file load
    */
   public float getLoad() {
      return (float)dataSize.get()/maxSize;
   }

   /**
    * Set the total byte of cache data available
    */
   public void setDataSize(AtomicInteger dataSize) {
      this.dataSize = dataSize;
   }

   /**
    * Increment the data size
    */
   public void incrementDataSize(int value) {
      dataSize.addAndGet(value);
   }

   /**
    * Decrement
    */
   public void decrementDataSize(int value) {
      dataSize.addAndGet(value * -1);
   }

   /**
    * Read the file headers and set his mode.
    * @return true if initialization completed successfully and file ready for usage. Otherwise, clears the file and return false 
    */
   public boolean init () throws Exception {

      try {

         readHeaders();

      }
      catch (Exception e){
         return false;
      }

      return true;

   }

   /**
    * Get the file size
    */
   public long getSize () throws Exception {
      return channel.size();
   }
   
   /**
    * Get the cache file name
    */
   public String getFilename() {
      return filename;
   }

   /**
    * Set the fileMode
    */
   public void setMode(int mode) throws Exception{
      this.mode = mode;
      writeHeaders();
   }

   /**
    * Return the file mode
    */
   public int getMode() {
      return mode;
   }
   
   /**
    * @return true if the file mode is ReadOnly
    */
   public boolean isReadOnly() {
      return (mode == READ_ONLY);
   }

   /**
    * Return the channel maximum size
    */
   public long getMaxSize() {
      return maxSize;
   }

   /**
    * Append data to the file
    * @param bb -- Byte Buffer wrapping the data
    * @return the offset where the data was written
    */
   public long appendData(ByteBuffer bb) throws Exception {

      if (isReadOnly()) {
         throw new Exception();
      }

      if (channel.size() + bb.array().length > maxSize) {
         return -1;
      }

      try {
         long offset = channel.position();
         channel.write(bb);
         return offset;

      } catch (Exception e) {
         throw new Exception();
      }
   }

   /**
    * Read next integer in the file from the given position
    */
   public int readInt(int pos) throws IOException {

      ByteBuffer bb = ByteBuffer.allocate(4);
      channel.read(bb, pos);
      bb.rewind();

      return bb.getInt();
   }
   
   /**
    * Read data from file at a given position
    * @param pos - position to read
    * @param len - length of data to read
    * @return ByteBuffer wrapping the data
    */
   public ByteBuffer readData(long pos, int len) throws IOException {

      readers.incrementAndGet();
      ByteBuffer bb = ByteBuffer.allocate(len);
      channel.read(bb, pos);
      bb.flip();

      readers.decrementAndGet();
      return bb;
   }

   /**
    * Read File Headers and set the file mode
    * If the file mode is not set, the file is cleared and an Exception is thrown
    */
   public void readHeaders ()  throws Exception {

      byte[] header = new byte[MAGIC.length];
      int mode;
      channel.position(0);
      if (channel.read(ByteBuffer.wrap(header), 0) == MAGIC.length && Arrays.equals(MAGIC, header)) {
         mode = readInt(MODE_POS);
         channel.position(channel.size());
      }
      else {
         clear();
         throw new Exception();
      }

      this.mode = mode;
   }

   /**
    * Write file Meta Data
    */
   public void writeHeaders () throws Exception {

      try {

         ByteBuffer bb = ByteBuffer.allocate(MAGIC.length + 4);
         bb.put(ByteBuffer.wrap(MAGIC));
         bb.putInt(mode);
         bb.flip();
         channel.write(bb, 0);
         channel.position(channel.size());

      }
      catch (Exception e){
         e.printStackTrace();
      }

   }

   public synchronized void flush() throws Exception {

      if (channel != null)
         channel.force(false);
   }
   
   /**
    * Increment the data size
    */
   public synchronized void clear() throws IOException {

      waitUnlocked();

      if (channel != null) {
         channel.truncate(0);
         channel.position(0);
      }

   }

   private synchronized void waitUnlocked() {
      while (readers.get() > 0) {
         try {
            wait();
         } catch (InterruptedException e) {
         }
      }
   }

   public synchronized void purge() throws IOException {

      if (channel != null) {

         channel.truncate(0);
         channel.position(0);
         Util.close(channel);
         channel = null;
      }

      if (file != null) {
         Util.close(file);
         file = null;
      }

   }

   @Override
   public int compareTo(Object o) {
      if (!(o instanceof FileStore))
         throw new ClassCastException();
      if (this == o)
         return 0;
      FileStore df = (FileStore) o;
      return this.getFilename().compareTo(df.getFilename());
   }

}