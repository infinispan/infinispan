package org.infinispan.server.hotrod;

import java.io.StreamCorruptedException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;

class UnknownVersionException extends StreamCorruptedException {
   final byte version;
   final long messageId;

   public UnknownVersionException(String cause, byte version, long messageId) {
      super(cause);
      this.version = version;
      this.messageId = messageId;
   }

   public HotRodHeader toHeader() {
      return new HotRodHeader(HotRodOperation.ERROR, version, messageId, "", 0, (short) 1, 0, MediaType.MATCH_ALL, MediaType.MATCH_ALL, null);
   }
}

class HotRodUnknownOperationException extends UnknownOperationException {
   final byte version;
   final long messageId;

   public HotRodUnknownOperationException(String cause, byte version, long messageId) {
      super(cause);
      this.version = version;
      this.messageId = messageId;
   }

   public HotRodHeader toHeader() {
      return new HotRodHeader(HotRodOperation.ERROR, version, messageId, "", 0, (short) 1, 0, MediaType.MATCH_ALL, MediaType.MATCH_ALL, null);
   }
}

class InvalidMagicIdException extends StreamCorruptedException {
   public InvalidMagicIdException(String cause) {
      super(cause);
   }
}

class CacheUnavailableException extends CacheException {

}

class RequestParsingException extends CacheException {
   final byte version;
   final long messageId;

   public RequestParsingException(String reason, byte version, long messageId, Exception cause) {
      super(reason, cause);
      this.version = version;
      this.messageId = messageId;
   }

   public RequestParsingException(String reason, byte version, long messageId) {
      super(reason);
      this.version = version;
      this.messageId = messageId;
   }

   public HotRodHeader toHeader() {
      return new HotRodHeader(HotRodOperation.ERROR, version, messageId, "", 0, (short) 1, 0, MediaType.MATCH_ALL, MediaType.MATCH_ALL, null);
   }
}

class CacheNotFoundException extends RequestParsingException {
   public CacheNotFoundException(String reason, byte version, long messageId) {
      super(reason, version, messageId);
   }
}

class UnknownOperationException extends StreamCorruptedException {
   public UnknownOperationException(String reason) {
      super(reason);
   }
}
