package org.infinispan.persistence.sifs;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Request for some change to be persisted in the Index or operation executed by index updater thread.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class IndexRequest {

   public enum Type {
      UPDATE,
      CLEAR,
      DELETE_FILE,
      STOP,
      GET_SIZE
   }

   private Type type = Type.UPDATE;
   private Object key;
   // the file and offset are duplicate to those in TemporaryTable because we have to match the CAS requests
   private int file;
   private int offset;
   private int prevFile = -1;
   private int prevOffset = -1;
   private byte[] serializedKey;
   private int size;
   private volatile Object result;
   private AtomicInteger countDown;

   public IndexRequest(Object key, byte[] serializedKey, int file, int offset, int size) {
      this.key = key;
      this.serializedKey = serializedKey;
      this.file = file;
      this.offset = offset;
      this.size = size;
   }

   public IndexRequest(Object key, byte[] serializedKey, int file, int offset, int size, int prevFile, int prevOffset) {
      this.key = key;
      this.serializedKey = serializedKey;
      this.file = file;
      this.offset = offset;
      this.size = size;
      this.prevFile = prevFile;
      this.prevOffset = prevOffset;
   }

   private IndexRequest(Type type) {
      this.type = type;
   }

   public static IndexRequest clearRequest() {
      return new IndexRequest(Type.CLEAR);
   }

   public static IndexRequest deleteFileRequest(int deletedFile) {
      IndexRequest req = new IndexRequest(Type.DELETE_FILE);
      req.file = deletedFile;
      return req;
   }

   public static IndexRequest stopRequest() {
      return new IndexRequest(Type.STOP);
   }

   public static IndexRequest sizeRequest() {
      return new IndexRequest(Type.GET_SIZE);
   }

   public Type getType() {
      return type;
   }

   public boolean isCompareAndSet() {
      return prevFile != -1;
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
            "file=" + file +
            ", offset=" + offset +
            ", prevFile=" + prevFile +
            ", prevOffset=" + prevOffset +
            ", size=" + size +
            ", type=" + type +
            '}';
   }
}
