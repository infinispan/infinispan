package org.infinispan.commons;

/**
 * Thrown when operations fail unexpectedly.
 * <p/>
 * Specific subclasses such as {@link org.infinispan.util.concurrent.TimeoutException} and {@link
 * org.infinispan.commons.CacheConfigurationException} have more specific uses.
 * <p/>
 * Transactions: if a CacheException (including any subclasses) is thrown for an operation on a JTA transaction, then
 * the transaction is marked for rollback.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a>
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class CacheException extends RuntimeException {

   /** The serialVersionUID */
   private static final long serialVersionUID = -5704354545244956536L;

   public CacheException() {
      super();
   }

   public CacheException(Throwable cause) {
      super(cause);
   }

   public CacheException(String msg) {
      super(msg);
   }

   public CacheException(String msg, Throwable cause) {
      super(msg, cause);
   }

   public CacheException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
   }
}
