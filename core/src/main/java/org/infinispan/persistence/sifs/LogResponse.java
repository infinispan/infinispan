package org.infinispan.persistence.sifs;

public class LogResponse {
   private final int id;
   private final int file;
   private final int offset;
   private final long expiration;

   public LogResponse(int id, int file, int offset, long expiration) {
      this.id = id;
      this.file = file;
      this.offset = offset;
      this.expiration = expiration;
   }

   public int getId() {
      return id;
   }

   public int getFile() {
      return file;
   }

   public int getOffset() {
      return offset;
   }

   public long getExpiration() {
      return expiration;
   }
}
