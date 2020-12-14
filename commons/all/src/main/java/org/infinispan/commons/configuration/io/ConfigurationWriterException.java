package org.infinispan.commons.configuration.io;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class ConfigurationWriterException extends RuntimeException {

   public ConfigurationWriterException(String message) {
      super(message);

   }

   public ConfigurationWriterException(Throwable t) {
      super(t);
   }
}
