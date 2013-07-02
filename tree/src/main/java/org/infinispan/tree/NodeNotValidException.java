package org.infinispan.tree;

import org.infinispan.commons.CacheException;

/**
 * Thrown whenever operations are attempted on a node that is no longer valid.  See {@link Node#isValid()} for details.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 4.0
 */
public class NodeNotValidException extends CacheException {
   private static final long serialVersionUID = 6576866180835456994L;

   public NodeNotValidException() {
   }

   public NodeNotValidException(Throwable cause) {
      super(cause);
   }

   public NodeNotValidException(String msg) {
      super(msg);
   }

   public NodeNotValidException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
