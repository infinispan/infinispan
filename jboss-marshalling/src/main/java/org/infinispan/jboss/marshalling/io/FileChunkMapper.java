package org.infinispan.jboss.marshalling.io;

import org.infinispan.Cache;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

/**
 * Takes care of properly storing and retrieving file chunks from/to cache.
 * Each chunk's key is composed of the file path and the chunk's number. The value is a byte array, which
 * is either chunkSize bytes long or less than that in the case of the last chunk.
 *
 * @author Marko Luksa
 * @deprecated since 10.0
 */
@Deprecated
class FileChunkMapper {

   private static final Log log = LogFactory.getLog(FileChunkMapper.class);
   private static final boolean trace = log.isTraceEnabled();

   private final GridFile file;
   private final Cache<String, byte[]> cache;

   public FileChunkMapper(GridFile file, Cache<String, byte[]> cache) {
      this.file = file;
      this.cache = cache;
   }

   /**
    * Guaranteed to be a power of two
    */
   public int getChunkSize() {
      return file.getChunkSize();
   }

   public byte[] fetchChunk(int chunkNumber) {
      String key = getChunkKey(chunkNumber);
      byte[] val = cache.get(key);
      if (trace)
         log.trace("fetching key=" + key + ": " + (val != null ? val.length + " bytes" : "null"));
      return val;
   }

   public void storeChunk(int chunkNumber, byte[] buffer, int length) {
      String key = getChunkKey(chunkNumber);
      byte[] val = trim(buffer, length);
      cache.put(key, val);
      if (trace)
         log.trace("put(): key=" + key + ": " + val.length + " bytes");
   }

   public void removeChunk(int chunkNumber) {
      cache.remove(getChunkKey(chunkNumber));
   }

   private byte[] trim(byte[] buffer, int length) {
      byte[] val = new byte[length];
      System.arraycopy(buffer, 0, val, 0, length);
      return val;
   }

   private String getChunkKey(int chunkNumber) {
      return getChunkKey(file.getAbsolutePath(), chunkNumber);
   }

   static String getChunkKey(String absoluteFilePath, int chunkNumber) {
      return absoluteFilePath + ".#" + chunkNumber;
   }
}
