package org.infinispan.factories.impl;

/**
 * Copy of {@link org.infinispan.factories.scopes.Scopes} to avoid a runtime dependency on the annotations module.
 *
 * @since 10.0
 */
public enum Scopes {
   /**
    * A single instance of the component is shared by all the caches.
    */
   GLOBAL,

   /**
    * Every cache uses a separate instance of the component.
    */
   NAMED_CACHE,

   /**
    * The component is not cached between requests, but a subclass may be either {@code GLOBAL} or {@code NAMED_CACHE}.
    */
   NONE
}
