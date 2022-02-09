package org.infinispan.api.annotations.indexing.option;

/**
 * Whether a field can be used in sorts.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.engine.backend.types.Sortable}
 */
public enum Sortable {

   /**
    * The field is not sortable.
    */
   NO,

   /**
    * The field is sortable
    */
   YES
}
