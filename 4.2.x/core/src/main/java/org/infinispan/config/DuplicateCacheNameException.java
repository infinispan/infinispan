package org.infinispan.config;

/**
 * Thrown if a duplicate named cache is detected
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DuplicateCacheNameException extends ConfigurationException {
   
   private static final long serialVersionUID = 3829520782638366878L;

   public DuplicateCacheNameException(Exception e) {
      super(e);
   }

   public DuplicateCacheNameException(String string) {
      super(string);
   }

   public DuplicateCacheNameException(String string, String erroneousAttribute) {
      super(string, erroneousAttribute);
   }

   public DuplicateCacheNameException(String string, Throwable throwable) {
      super(string, throwable);
   }
}
