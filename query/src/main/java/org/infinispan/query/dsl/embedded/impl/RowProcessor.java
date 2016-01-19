package org.infinispan.query.dsl.embedded.impl;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
public interface RowProcessor extends ResultProcessor<Object[], Object[]> {

   /**
    * Apply data conversion. The input row can be modified in-place or a new one, of equal or different size, can be
    * created.
    *
    * @param row the input row (never {@code null})
    * @return may return the input row or a new one
    */
   Object[] process(Object[] row);
}
