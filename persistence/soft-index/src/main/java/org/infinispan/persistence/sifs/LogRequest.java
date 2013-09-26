package org.infinispan.persistence.sifs;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.marshall.core.MarshalledEntry;

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
      STOP,
      PAUSE
   }

   private final Type type;
   private final Object key;
   private final long expirationTime;
   private final ByteBuffer serializedKey;
   private final ByteBuffer serializedMetadata;
   private final ByteBuffer serializedValue;
   private boolean canContinue = false;
   private volatile IndexRequest indexRequest;

   private LogRequest(Type type, Object key, long expirationTime, ByteBuffer serializedKey, ByteBuffer serializedMetadata, ByteBuffer serializedValue) {
      this.key = key;
      this.expirationTime = expirationTime;
      this.serializedKey = serializedKey;
      this.serializedMetadata = serializedMetadata;
      this.serializedValue = serializedValue;
      this.type = type;
   }

   private LogRequest(Type type) {
      this(type, null, 0, null, null, null);
   }

   public static LogRequest storeRequest(MarshalledEntry entry) throws IOException, InterruptedException {
      return new LogRequest(Type.STORE, entry.getKey(),
            entry.getMetadata() == null ? -1 : entry.getMetadata().expiryTime(),
            entry.getKeyBytes(), entry.getMetadataBytes(), entry.getValueBytes());
   }

   public static LogRequest deleteRequest(Object key, ByteBuffer serializedKey) throws IOException, InterruptedException {
      return new LogRequest(Type.DELETE, key, -1, serializedKey, null, null);
   }

   public static LogRequest clearRequest() {
      return new LogRequest(Type.CLEAR_ALL);
   }

   public static LogRequest stopRequest() {
      return new LogRequest(Type.STOP);
   }

   public static LogRequest pauseRequest() {
      return new LogRequest(Type.PAUSE);
   }

   public int length() {
      return EntryHeader.HEADER_SIZE + serializedKey.getLength()
            + (serializedValue != null ? serializedValue.getLength() : 0)
            + (serializedMetadata != null ? serializedMetadata.getLength() : 0);
   }

   public Object getKey() {
      return key;
   }

   public ByteBuffer getSerializedKey() {
      return serializedKey;
   }

   public ByteBuffer getSerializedMetadata() {
      return serializedMetadata;
   }

   public ByteBuffer getSerializedValue() {
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

   public boolean isPause() {
      return type == Type.PAUSE;
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
