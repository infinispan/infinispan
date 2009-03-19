package org.horizon.config;

/**
 * Thrown if a duplicate named cache is detected
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DuplicateCacheNameException extends ConfigurationException {
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
