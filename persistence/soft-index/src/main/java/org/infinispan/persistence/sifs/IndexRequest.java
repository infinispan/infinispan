package org.infinispan.persistence.sifs;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.Util;

/**
 * Request for some change to be persisted in the Index or operation executed by index updater thread.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class IndexRequest {
   public enum Type {
      UPDATE,
      MOVED,
      DROPPED,
      FOUND_OLD,
      CLEAR,
      DELETE_FILE,
      STOP,
      GET_SIZE
   }

   private final Type type;
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

   private IndexRequest(Type type, Object key, byte[] serializedKey, int file, int offset, int size, int prevFile, int prevOffset) {
      this.type = type;
      this.key = key;
      this.file = file;
      this.offset = offset;
      this.prevFile = prevFile;
      this.prevOffset = prevOffset;
      this.serializedKey = serializedKey;
      this.size = size;
   }

   public static IndexRequest update(Object key, byte[] serializedKey, int file, int offset, int size) {
      return new IndexRequest(Type.UPDATE, key, serializedKey, file, offset, size, -1, -1);
   }

   public static IndexRequest moved(Object key, byte[] serializedKey, int file, int offset, int size, int prevFile, int prevOffset) {
      return new IndexRequest(Type.MOVED, key, serializedKey, file, offset, size, prevFile, prevOffset);
   }

   public static IndexRequest dropped(Object key, byte[] serializedKey, int prevFile, int prevOffset) {
      return new IndexRequest(Type.DROPPED, key, serializedKey, -1, -1, -1, prevFile, prevOffset);
   }

   public static IndexRequest foundOld(Object key, byte[] serializedKey, int prevFile, int prevOffset) {
      return new IndexRequest(Type.FOUND_OLD, key, serializedKey, -1, -1, -1, prevFile, prevOffset);
   }

   public static IndexRequest clearRequest() {
      return new IndexRequest(Type.CLEAR, null, null, -1, -1, -1, -1, -1);
   }

   public static IndexRequest deleteFileRequest(int deletedFile) {
      return new IndexRequest(Type.DELETE_FILE, null, null, deletedFile, -1, -1, -1, -1);
   }

   public static IndexRequest stopRequest() {
      return new IndexRequest(Type.STOP, null, null, -1, -1, -1, -1, -1);
   }

   public static IndexRequest sizeRequest() {
      return new IndexRequest(Type.GET_SIZE, null, null, -1, -1, -1, -1, -1);
   }

   public Type getType() {
      return type;
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
