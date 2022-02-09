package org.infinispan.api.annotations.indexing.option;

/**
 * Whether we want to be able to obtain the value of the field as a projection.
 * <p>
 * This usually means that the field will be stored in the index, but it is more subtle than that, for instance in the
 * case of projection by distance.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.engine.backend.types.Projectable}
 */
public enum Projectable {

   /**
    * Do not allow projection on the field.
    */
   NO,

   /**
    * Allow projection on the field.
    */
   YES
}
