package org.infinispan.distexec.mapreduce;

import org.infinispan.commons.CacheException;

/**
 * An exception indicating Map/Reduce job failure
 *
 * @author Vladimir Blagojevic
 * @since 7.0
 */
public class MapReduceException extends CacheException {

   private static final long serialVersionUID = -1699361066674350664L;

   public MapReduceException(Throwable cause) {
      super(cause);
   }

   public MapReduceException(String message, Throwable cause) {
      super(message, cause);
   }

   public MapReduceException(String message) {
      super(message);
   }
}
