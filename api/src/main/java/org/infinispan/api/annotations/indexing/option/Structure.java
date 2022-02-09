package org.infinispan.api.annotations.indexing.option;

/**
 * Defines how the structure of an object field is preserved upon indexing.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.engine.backend.types.ObjectStructure}
 */
public enum Structure {

   /**
    * Flatten multi-valued object fields.
    * <p>
    * This structure is generally more efficient,
    * but has the disadvantage of dropping the original structure
    * by making the leaf fields multi-valued instead of the object fields.
    */
   FLATTENED,

   /**
    * Store object fields as nested documents.
    * <p>
    * This structure is generally less efficient,
    * but has the advantage of preserving the original structure.
    * Note however that access to that information when querying
    * requires special care.
    */
   NESTED

}
