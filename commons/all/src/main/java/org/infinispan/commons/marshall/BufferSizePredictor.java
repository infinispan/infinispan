package org.infinispan.commons.marshall;

/**
 * Buffer size predictor
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public interface BufferSizePredictor {

   /**
    * Provide the next buffer size taking in account
    * the object to store in the buffer.
    *
    * @param obj instance that will be stored in the buffer
    * @return int representing the next predicted buffer size
    */
   int nextSize(Object obj);

   /**
    * Record the size of the of data in the last buffer used.
    *
    * @param previousSize int representing the size of the last
    *                         object buffered.
    */
   void recordSize(int previousSize);

}
