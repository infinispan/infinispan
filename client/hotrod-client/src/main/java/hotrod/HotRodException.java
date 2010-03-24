package hotrod;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class HotRodException extends RuntimeException {
   public HotRodException() {
   }

   public HotRodException(String message) {
      super(message);
   }

   public HotRodException(String message, Throwable cause) {
      super(message, cause);
   }

   public HotRodException(Throwable cause) {
      super(cause);
   }
}
