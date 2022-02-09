package org.infinispan.api.annotations.indexing.option;

/**
 * Whether index-time scoring information for the field should be stored or not.
 * <p>
 * Enabling norms will improve the quality of scoring.
 * Disabling norms will reduce the disk space used by the index.
 * <p>
 * Simplified version for Infinispan of {@link org.hibernate.search.engine.backend.types.Norms}
 *
 * @since 14.0
 */
public enum Norms {

   /**
    * The index-time scoring information is not stored.
    */
   NO,

   /**
    * The index-time scoring information is stored.
    */
   YES

}
