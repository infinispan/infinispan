package org.infinispan.commons.configuration.io;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class ConfigurationReaderException extends RuntimeException {
   private final Location location;

   public ConfigurationReaderException(String message, Location location) {
      super(message + location);
      this.location = location;
   }

   public ConfigurationReaderException(Throwable t, Location location) {
      super(t);
      this.location = location;
   }

   public Location getLocation() {
      return location;
   }
}
