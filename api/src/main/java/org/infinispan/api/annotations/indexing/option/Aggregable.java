package org.infinispan.api.annotations.indexing.option;

/**
 * Whether the field can be used in projections.
 * <p>
 * This usually means that the field will have doc-values stored in the index.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.engine.backend.types.Aggregable}
 */
public enum Aggregable {

   /**
    * Do not allow aggregation on the field.
    */
   NO,

   /**
    * Allow aggregation on the field.
    */
   YES
}
