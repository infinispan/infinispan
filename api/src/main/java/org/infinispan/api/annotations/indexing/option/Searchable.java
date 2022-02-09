package org.infinispan.api.annotations.indexing.option;

/**
 * Whether we want to be able to search the document using this field.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.engine.backend.types.Searchable}
 */
public enum Searchable {

   /**
    * The field is not searchable, i.e. search predicates cannot be applied to the field.
    * <p>
    * This can save some disk space if you know the field is only used for projections or sorts.
    */
   NO,

   /**
    * The field is searchable, i.e. search predicates can be applied to the field.
    */
   YES
}
