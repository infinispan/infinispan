package org.infinispan.server.websocket.json;

/**
 * Occurs if there is a problem while converting from String to JSON.
 *
 * @author Sebastian Laskawiec
 */
public class JsonConversionException extends Exception {

   public JsonConversionException(String message, Throwable t) {
      super(message, t);
   }
}
