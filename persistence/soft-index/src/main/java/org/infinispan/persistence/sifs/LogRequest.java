package org.infinispan.persistence.sifs;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.persistence.spi.MarshallableEntry;

/**
 * Request to persist entry in log file or request executed by the log appender thread.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @author William Burns &lt;wburns@redhat.com&gt;
 */
class LogRequest extends CompletableFuture<Void> {

   enum Type {
      STORE,
      DELETE,
      CLEAR_ALL,
      RESUME
   }

   private final Type type;
   private final Object key;
   private final long expirationTime;
   private final ByteBuffer serializedKey;
   private final ByteBuffer serializedMetadata;
   private final ByteBuffer serializedValue;
   private final ByteBuffer serializedInternalMetadata;
   private final long created;
   private final long lastUsed;
   private volatile IndexRequest indexRequest;

   private LogRequest(Type type, Object key, long expirationTime, ByteBuffer serializedKey, ByteBuffer serializedMetadata,
                      ByteBuffer serializedInternalMetadata, ByteBuffer serializedValue, long created, long lastUsed) {
      this.key = key;
      this.expirationTime = expirationTime;
      this.serializedKey = serializedKey;
      this.serializedMetadata = serializedMetadata;
      this.serializedInternalMetadata = serializedInternalMetadata;
      this.serializedValue = serializedValue;
      this.created = created;
      this.lastUsed = lastUsed;
      this.type = type;
   }

   private LogRequest(Type type) {
      this(type, null, 0, null, null, null, null, -1, -1);
   }

   public static LogRequest storeRequest(MarshallableEntry entry) {
      return new LogRequest(Type.STORE, entry.getKey(), entry.expiryTime(), entry.getKeyBytes(), entry.getMetadataBytes(),
            entry.getInternalMetadataBytes(), entry.getValueBytes(), entry.created(), entry.lastUsed());
   }

   public static LogRequest deleteRequest(Object key, ByteBuffer serializedKey) {
      return new LogRequest(Type.DELETE, key, -1, serializedKey, null, null, null, -1, -1);
   }

   public static LogRequest clearRequest() {
      return new LogRequest(Type.CLEAR_ALL);
   }

   public static LogRequest resumeRequest() {
      return new LogRequest(Type.RESUME);
   }

   public int length() {
      return EntryHeader.HEADER_SIZE_11_0 + serializedKey.getLength()
            + (serializedValue != null ? serializedValue.getLength() : 0)
            + EntryMetadata.size(serializedMetadata)
            + (serializedInternalMetadata != null ? serializedInternalMetadata.getLength() : 0);
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

   public ByteBuffer getSerializedInternalMetadata() {
      return serializedInternalMetadata;
   }

   public ByteBuffer getSerializedValue() {
      return serializedValue;
   }

   public long getCreated() {
      return created;
   }

   public long getLastUsed() {
      return lastUsed;
   }

   public long getExpiration() {
      return expirationTime;
   }

   public boolean isClear() {
      return type == Type.CLEAR_ALL;
   }

   public boolean isResume() {
      return type == Type.RESUME;
   }

   public void setIndexRequest(IndexRequest indexRequest) {
      this.indexRequest = indexRequest;
   }

   public IndexRequest getIndexRequest() {
      return indexRequest;
   }
}
