package org.infinispan.util;

/**
 * Thrown when client's code passed as a labda expression in commands such as {@link org.infinispan.commands.write.ComputeIfAbsentCommand}
 * raises a exception. We don't want to convert this excepton into a {@link org.infinispan.commons.CacheException} but
 * instead just propagate it to the user as it is.
 *
 * @author karesti@redhat.com
 * @since 9.1
 */
public class UserRaisedFunctionalException extends RuntimeException {

   public UserRaisedFunctionalException(Throwable cause) {
      super(cause);
   }
}
