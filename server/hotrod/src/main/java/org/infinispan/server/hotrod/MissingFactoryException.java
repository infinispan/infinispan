package org.infinispan.server.hotrod;

/**
 * Exception thrown when a named factory is chosen that doesn't exist
 *
 * @author wburns
 * @since 9.0
 */
public class MissingFactoryException extends IllegalArgumentException {
   public MissingFactoryException(String reason) {
      super(reason);
   }
}
