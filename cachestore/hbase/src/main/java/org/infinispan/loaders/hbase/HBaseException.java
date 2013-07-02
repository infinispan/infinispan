package org.infinispan.loaders.hbase;

/**
 * 
 * Class representing an exception from HBase.
 * 
 * @author Justin Hayes
 * @since 5.2
 */
public class HBaseException extends Exception {

   private static final long serialVersionUID = -1314731405790339426L;

   public HBaseException(String message) {
      super(message);
   }

   public HBaseException(String message, Throwable t) {
      super(message, t);
   }

}
