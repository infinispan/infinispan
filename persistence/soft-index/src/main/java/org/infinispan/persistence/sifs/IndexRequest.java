package org.infinispan.persistence.sifs;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.Util;

/**
 * Request for some change to be persisted in the Index or operation executed by index updater thread.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class IndexRequest extends CompletableFuture<Object> {
   public enum Type {
      UPDATE,
      MOVED,
      DROPPED,
      FOUND_OLD,
      CLEAR,
      // TODO: this can probably be removed and just let compactor delete the file - no reason for index to do that
      DELETE_FILE,
      // TODO: NEED TO REMOVE THIS
      STOP,
      SIZE
   }

   private final Type type;
   private final int segment;
   private final Object key;
   // the file and offset are duplicate to those in TemporaryTable because we have to match the CAS requests
   private final int file;
   private final int offset;
   private final int prevFile;
   private final int prevOffset;
   private final byte[] serializedKey;
   private final int size;
   private volatile Object result;
   private AtomicInteger countDown;

   private IndexRequest(Type type, int segment, Object key, byte[] serializedKey, int file, int offset, int size, int prevFile, int prevOffset) {
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

   public static IndexRequest update(int segment, Object key, byte[] serializedKey, int file, int offset, int size) {
      return new IndexRequest(Type.UPDATE, segment, Objects.requireNonNull(key), serializedKey, file, offset, size, -1, -1);
   }

   public static IndexRequest moved(int segment, Object key, byte[] serializedKey, int file, int offset, int size, int prevFile, int prevOffset) {
      return new IndexRequest(Type.MOVED, segment, Objects.requireNonNull(key), serializedKey, file, offset, size, prevFile, prevOffset);
   }

   public static IndexRequest dropped(int segment, Object key, byte[] serializedKey, int prevFile, int prevOffset) {
      return new IndexRequest(Type.DROPPED, segment, Objects.requireNonNull(key), serializedKey, -1, -1, -1, prevFile, prevOffset);
   }

   public static IndexRequest foundOld(int segment, Object key, byte[] serializedKey, int prevFile, int prevOffset) {
      return new IndexRequest(Type.FOUND_OLD, segment, Objects.requireNonNull(key), serializedKey, -1, -1, -1, prevFile, prevOffset);
   }

   public static IndexRequest clearRequest() {
      return new IndexRequest(Type.CLEAR, -1, null, null, -1, -1, -1, -1, -1);
   }

   public static IndexRequest deleteFileRequest(int deletedFile) {
      return new IndexRequest(Type.DELETE_FILE, -1, null, null, deletedFile, -1, -1, -1, -1);
   }

   public static IndexRequest stopRequest() {
      return new IndexRequest(Type.STOP, -1,null, null, -1, -1, -1, -1, -1);
   }

   public static IndexRequest sizeRequest() {
      return new IndexRequest(Type.SIZE, -1,null, null, -1, -1, -1, -1, -1);
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

   public byte[] getSerializedKey() {
      return serializedKey;
   }

   public synchronized void setResult(Object result) {
      if (this.result == null) {
         this.result = result;
      }
      notifyAll();
   }


   public synchronized Object getResult() throws InterruptedException {
      while (result == null) {
         wait();
      }
      return result;
   }

   // TODO: delete this when replaced
   public void setCountDown(int countDown) {
      this.countDown = new AtomicInteger(countDown);
   }

   public boolean countDown() {
      return countDown.decrementAndGet() == 0;
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
            "key=" + key +
            ", serializedKey=" + Util.printArray(serializedKey) +
            ", file=" + file +
            ", offset=" + offset +
            ", prevFile=" + prevFile +
            ", prevOffset=" + prevOffset +
            ", size=" + size +
            ", type=" + type +
            '}';
   }
}
