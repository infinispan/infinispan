package org.infinispan.persistence.sifs;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.util.Util;

/**
 * Request for some change to be persisted in the Index or operation executed by index updater thread.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class IndexRequest extends CompletableFuture<Object> {
   public enum Type {
      UPDATE,
      UPDATE_REMOVAL,
      MOVED,
      DROPPED,
      FOUND_OLD,
      CLEAR,
      SYNC_REQUEST,
   }

   private final Type type;
   private final int segment;
   private final Object key;
   // the file and offset are duplicate to those in TemporaryTable because we have to match the CAS requests
   private final int file;
   private final int offset;
   private final int prevFile;
   private final int prevOffset;
   private final ByteBuffer serializedKey;
   private final int size;

   private IndexRequest(Type type, int segment, Object key, ByteBuffer serializedKey, int file, int offset, int size, int prevFile, int prevOffset) {
      this.type = type;
      this.segment = segment;
      this.key = key;
      this.file = file;
      this.offset = offset;
      this.prevFile = prevFile;
      this.prevOffset = prevOffset;
      this.serializedKey = serializedKey;
      this.size = size;
   }

   public static IndexRequest update(int segment, Object key, ByteBuffer serializedKey, boolean isRemove, int file, int offset, int size) {
      return new IndexRequest(isRemove ? Type.UPDATE_REMOVAL : Type.UPDATE, segment, Objects.requireNonNull(key), serializedKey, file, offset, size, -1, -1);
   }

   public static IndexRequest moved(int segment, Object key, ByteBuffer serializedKey, int file, int offset, int size, int prevFile, int prevOffset) {
      return new IndexRequest(Type.MOVED, segment, Objects.requireNonNull(key), serializedKey, file, offset, size, prevFile, prevOffset);
   }

   public static IndexRequest dropped(int segment, Object key, ByteBuffer serializedKey, int file, int offset, int prevFile, int prevOffset) {
      return new IndexRequest(Type.DROPPED, segment, Objects.requireNonNull(key), serializedKey, file, offset, -1, prevFile, prevOffset);
   }

   public static IndexRequest foundOld(int segment, Object key, ByteBuffer serializedKey, int file, int offset, int size) {
      return new IndexRequest(Type.FOUND_OLD, segment, Objects.requireNonNull(key), serializedKey, file, offset, size, -1, -1);
   }

   public static IndexRequest clearRequest() {
      return new IndexRequest(Type.CLEAR, -1, null, null, -1, -1, -1, -1, -1);
   }

   /**
    * Allows for an index request that will be ran in the index thread. This can be useful to run something after all
    * pending index updates have been applied.
    * @param runnable what will be ran in the index thread after all pending index updates are applied first
    * @return the request
    */
   public static IndexRequest syncRequest(Runnable runnable) {
      return new IndexRequest(Type.SYNC_REQUEST, -1, runnable, null, -1, -1, -1, -1, -1);
   }

   public Type getType() {
      return type;
   }

   public int getSegment() {
      return segment;
   }

   public Object getKey() {
      return key;
   }

   public long getPrevFile() {
      return prevFile;
   }

   public int getPrevOffset() {
      return prevOffset;
   }

   public ByteBuffer getSerializedKey() {
      return serializedKey;
   }

   public int getFile() {
      return file;
   }

   public int getOffset() {
      return offset;
   }

   public int getSize() {
      return size;
   }

   @Override
   public String toString() {
      return "IndexRequest{" +
            "key=" + Util.toStr(key) +
            ", serializedKey=" + serializedKey +
            ", cacheSegment=" + segment +
            ", file=" + file +
            ", offset=" + offset +
            ", prevFile=" + prevFile +
            ", prevOffset=" + prevOffset +
            ", size=" + size +
            ", type=" + type +
            '}';
   }
}
