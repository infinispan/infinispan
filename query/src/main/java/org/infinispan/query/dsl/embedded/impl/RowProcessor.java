package org.infinispan.query.dsl.embedded.impl;

/**
 * A {@link ResultProcessor} that processes projections (rows).
 *
 * @author anistor@redhat.com
 * @since 8.1
 */
@FunctionalInterface
public interface RowProcessor extends ResultProcessor<Object[], Object[]> {

   /**
    * Apply data conversion. The input row can be modified in-place or a new one, of equal or different size, can be
    * created. Some of the possible conversions are type casts and the processing of null markers.
    *
    * @param row the input row (never {@code null})
    * @return may return the input row or a newly created one
    */
   Object[] process(Object[] row);
}
