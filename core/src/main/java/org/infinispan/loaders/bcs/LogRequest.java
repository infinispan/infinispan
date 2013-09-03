package org.infinispan.loaders.bcs;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.container.entries.InternalCacheEntry;

import java.io.IOException;

/**
 * Request to persist entry in log file or request executed by the log appender thread.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class LogRequest {
   enum Type {
      STORE,
      DELETE,
      CLEAR_ALL,
      STOP
   }

   private final Type type;
   private final Object key;
   private final long expirationTime;
   private final byte[] serializedKey;
   private final byte[] serializedValue;
   private boolean canContinue = false;
   private IndexRequest indexRequest;

   private LogRequest(Type type, Object key, long expirationTime, byte[] serializedKey, byte[] serializedValue) {
      this.key = key;
      this.expirationTime = expirationTime;
      this.serializedKey = serializedKey;
      this.serializedValue = serializedValue;
      this.type = type;
   }

   public static LogRequest storeRequest(InternalCacheEntry entry, Marshaller marshaller) throws IOException, InterruptedException {
      return new LogRequest(Type.STORE, entry.getKey(), entry.getExpiryTime(),
            marshaller.objectToByteBuffer(entry.getKey()),
            marshaller.objectToByteBuffer(entry.toInternalCacheValue()));
   }

   public static LogRequest deleteRequest(Object key, Marshaller marshaller) throws IOException, InterruptedException {
      return new LogRequest(Type.DELETE, key, -1, marshaller.objectToByteBuffer(key), null);
   }

   public static LogRequest clearRequest() {
      return new LogRequest(Type.CLEAR_ALL, null, 0, null, null);
   }

   public static LogRequest stopRequest() {
      return new LogRequest(Type.STOP, null, 0, null, null);
   }

   public int length() {
      return serializedKey.length + (serializedValue != null ? serializedValue.length : 0) + EntryReaderWriter.HEADER_SIZE;
   }

   public Object getKey() {
      return key;
   }

   public byte[] getSerializedKey() {
      return serializedKey;
   }

   public byte[] getSerializedValue() {
      return serializedValue;
   }

   public long getExpiration() {
      return expirationTime;
   }

   public boolean isClear() {
      return type == Type.CLEAR_ALL;
   }

   public boolean isStop() {
      return type == Type.STOP;
   }

   public void setIndexRequest(IndexRequest indexRequest) {
      this.indexRequest = indexRequest;
   }

   public IndexRequest getIndexRequest() {
      return indexRequest;
   }

   public synchronized void pause() throws InterruptedException {
      while (!canContinue) wait();
   }

   public synchronized void resume() {
      canContinue = true;
      notify();
   }
}
