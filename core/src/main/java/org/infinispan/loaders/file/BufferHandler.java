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

import java.nio.ByteBuffer;

import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.marshall.StreamingMarshaller;

/**
 * 
 * @author Patrick Azogni
 *
 */
public class BufferHandler {

   StreamingMarshaller marshaller;
   
   public BufferHandler(StreamingMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   /**
    * Compose bytebuffer wrapping byte array containing data to be written in the file
    * @param mode - Is is it add or delete
    * @param keyHash - Hash Value of key
    * @param keyByte - Key in byte array
    * @param valueByte - Value in byte array
    * @param expiryTime - Entry expiry time
    * 
    */
   public ByteBuffer compose(int mode, int keyHash, byte[] keyByte, byte[] valueByte, long expiryTime) {

      int len;
      len = (valueByte == null)?FileStore.MINIMUM_LENGTH:FileStore.MINIMUM_LENGTH_INSERT + keyByte.length + valueByte.length;

      ByteBuffer bb = ByteBuffer.allocate(len);
      bb.putInt(mode);
      bb.putInt(len);
      bb.putInt(keyHash);
      bb.putLong(expiryTime);
      if (keyByte != null){
         bb.putInt(keyByte.length);
         bb.put(keyByte);
         bb.putInt(valueByte.length);
         bb.put(valueByte);
      }
      bb.flip();

      return bb;
   }
   
   /**
    * Extracts cache key from a byte buffer
    */
   public Object getKeyFromBuffer(ByteBuffer bb) throws CacheLoaderException {

      try {
         bb.position(FileStore.KEY_LEN_POS);
         int keyLen = bb.getInt();
         byte[] keyByte = new byte[keyLen];
         System.arraycopy(bb.array(), FileStore.KEY_LEN_POS + 4, keyByte, 0, keyLen);
         Object key = marshaller.objectFromByteBuffer(keyByte);

         return key;
      } catch (Exception e) {
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Extracts cache value from a byte buffer
    */
   public InternalCacheValue getValueFromBuffer(ByteBuffer bb) throws CacheLoaderException {

      try {
         bb.position(FileStore.KEY_LEN_POS);
         int keyLen = bb.getInt();

         int value_len_pos = FileStore.KEY_LEN_POS + keyLen + 4;
         bb.position(value_len_pos);
         int valueLen = bb.getInt();
         byte[] valueByte = new byte[valueLen];

         int value_pos = value_len_pos + 4;
         System.arraycopy(bb.array(), value_pos, valueByte, 0, valueLen);
         InternalCacheValue value = (InternalCacheValue) marshaller.objectFromByteBuffer(valueByte);

         return value;
      } catch (Exception e) {
         e.printStackTrace();
         throw new CacheLoaderException(e);
      }

   }

}
