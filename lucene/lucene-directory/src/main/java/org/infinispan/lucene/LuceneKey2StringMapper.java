package org.infinispan.lucene;

import java.util.regex.Pattern;

import org.infinispan.persistence.keymappers.TwoWayKey2StringMapper;
import org.infinispan.lucene.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * To configure a JdbcStringBasedCacheStoreConfig for the Lucene Directory, use this
 * Key2StringMapper implementation.
 *
 * @see org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder#key2StringMapper(String)
 *
 * @author Sanne Grinovero
 * @since 4.1
 */
@SuppressWarnings("unchecked")
public final class LuceneKey2StringMapper implements TwoWayKey2StringMapper {

   /**
    * The pipe character was chosen as it's illegal to have a pipe in a filename, so we only have to
    * check for the indexnames.
    */
   static final Pattern singlePipePattern = Pattern.compile("\\|");

   private static final Log log = LogFactory.getLog(LuceneKey2StringMapper.class, Log.class);

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
      }
      else {
         String[] split = singlePipePattern.split(key);
         if (split.length != 3 && split.length != 4) {
            throw log.keyMappperUnexpectedStringFormat(key);
         }
         else {
            switch (split[1]) {
               case "M":
                  if (split.length != 3) {
                     throw log.keyMappperUnexpectedStringFormat(key);
                  }
                  return new FileCacheKey(split[2], split[0]);
               case "RL":
                  if (split.length != 3) throw log.keyMappperUnexpectedStringFormat(key);
                  return new FileReadLockKey(split[2], split[0]);
               default:
                  if (split.length != 4) throw log.keyMappperUnexpectedStringFormat(key);
                  try {
                     int chunkId = Integer.parseInt(split[1]);
                     int bufferSize = Integer.parseInt(split[2]);
                     return new ChunkCacheKey(split[3], split[0], chunkId, bufferSize);
                  }
                  catch (NumberFormatException nfe) {
                     throw log.keyMappperUnexpectedStringFormat(key);
                  }
            }
         }
      }
   }

}
