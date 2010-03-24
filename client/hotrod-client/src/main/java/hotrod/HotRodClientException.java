package hotrod;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class HotRodClientException extends HotRodException {
   private long messageId;
   private int errorStatusCode;

   public HotRodClientException(String message, Throwable cause) {
      super(message, cause);
   }

   public HotRodClientException(long messageId, int errorStatusCode) {
      this.messageId = messageId;
      this.errorStatusCode = errorStatusCode;
   }


   @Override
   public String toString() {
      return "HotRodServerException{" +
            "messageId=" + messageId +
            ", errorStatusCode=" + errorStatusCode +
            "} " + super.toString();
   }

   @Override
   public String getMessage() {
      return toString() + "." + super.getMessage();    
   }
}
