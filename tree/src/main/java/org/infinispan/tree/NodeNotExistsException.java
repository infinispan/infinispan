package org.infinispan.tree;

import org.infinispan.commons.CacheException;


/**
 * Thrown when an operation is attempted on a non-existing node in the cache
 *
 * @author <a href="mailto:bela@jboss.com">Bela Ban</a>.
 * @since 4.0
 */

public class NodeNotExistsException extends CacheException {

   private static final long serialVersionUID = 779376138690777440L;

   public NodeNotExistsException() {
      super();
   }


   public NodeNotExistsException(String msg) {
      super(msg);
   }


   public NodeNotExistsException(String msg, Throwable cause) {
      super(msg, cause);
   }


}
