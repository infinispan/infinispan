package org.infinispan.server.hotrod;

import java.io.IOException;
import java.io.StreamCorruptedException;

public class HotRodUtils {
}

class UnknownVersionException extends StreamCorruptedException {
   final byte version;
   final long messageId;

   public UnknownVersionException(String cause, byte version, long messageId) {
      super(cause);
      this.version = version;
      this.messageId = messageId;
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
}

class InvalidMagicIdException extends StreamCorruptedException {
   public InvalidMagicIdException(String cause) {
      super(cause);
   }
}

class CacheUnavailableException extends Exception {

}

class RequestParsingException extends IOException {
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
}

class CacheNotFoundException extends RequestParsingException {
   public CacheNotFoundException(String reason, byte version, long messageId) {
      super(reason, version, messageId);
   }
}

class HotRodException extends Exception {
   final ErrorResponse response;
   final Throwable cause;

   public HotRodException(ErrorResponse response, String message, Throwable cause) {
      super(message);
      this.response = response;
      this.cause = cause;
   }
}

class UnknownOperationException extends StreamCorruptedException {
   public UnknownOperationException(String reason) {
      super(reason);
   }
}
