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
package org.infinispan.lucene;

import java.util.regex.Pattern;

import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.infinispan.loaders.keymappers.TwoWayKey2StringMapper;
import org.infinispan.lucene.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * To configure a JdbcStringBasedCacheStoreConfig for the Lucene Directory, use this
 * Key2StringMapper implementation.
 * 
 * @see JdbcStringBasedCacheStoreConfig#setKey2StringMapperClass(String)
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@SuppressWarnings("unchecked")
public class LuceneKey2StringMapper implements TwoWayKey2StringMapper {

   private static final Log log = LogFactory.getLog(LuceneKey2StringMapper.class, Log.class);

   /**
    * The pipe character was chosen as it's illegal to have a pipe in a filename, so we only have to
    * check for the indexnames.
    */
   static final Pattern singlePipePattern = Pattern.compile("\\|");

   @Override
   public boolean isSupportedType(Class<?> keyType) {
      return (keyType == ChunkCacheKey.class    ||
              keyType == FileCacheKey.class     ||
              keyType == FileListCacheKey.class ||
              keyType == FileReadLockKey.class);
   }

   @Override
   public String getStringMapping(Object key) {
      return key.toString();
   }

   /**
    * This method has to perform the inverse transformation of the keys used in the Lucene
    * Directory from String to object. So this implementation is strongly coupled to the 
    * toString method of each key type.
    * 
    * @see ChunkCacheKey#toString()
    * @see FileCacheKey#toString()
    * @see FileListCacheKey#toString()
    * @see FileReadLockKey#toString()
    */
   @Override
   public Object getKeyMapping(String key) {
      if (key == null) {
         throw new IllegalArgumentException("Not supporting null keys");
      }
      // ChunkCacheKey: fileName + "|" + chunkId + "|" + bufferSize "|" + indexName
      // FileCacheKey : fileName + "|M|"+ indexName;
      // FileListCacheKey : "*|" + indexName;
      // FileReadLockKey : fileName + "|RL|"+ indexName;
      if (key.startsWith("*|")) {
         return new FileListCacheKey(key.substring(2));
      } else {
         String[] split = singlePipePattern.split(key);
         if (split.length != 3 && split.length != 4) {
            throw log.keyMappperUnexpectedStringFormat(key);
         } else {
            if ("M".equals(split[1])) {
               if (split.length != 3) {
                  throw log.keyMappperUnexpectedStringFormat(key);
               }
               return new FileCacheKey(split[2], split[0]);
            } else if ("RL".equals(split[1])) {
               if (split.length != 3) throw log.keyMappperUnexpectedStringFormat(key);
               return new FileReadLockKey(split[2], split[0]);
            } else {
               if (split.length != 4) throw log.keyMappperUnexpectedStringFormat(key);
               try {
                  int chunkId = Integer.parseInt(split[1]);
                  int bufferSize = Integer.parseInt(split[2]);
                  return new ChunkCacheKey(split[3], split[0], chunkId, bufferSize);
               } catch (NumberFormatException nfe) {
                  throw log.keyMappperUnexpectedStringFormat(key);
               }
            }
         }
      }
   }
   
}
