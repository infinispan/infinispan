package org.infinispan.factories.scopes;

/**
 * The different scopes that can be declared for a component.
 *
 * <p>Must be kept in sync with {@code org.infinispan.factories.impl.Scopes}</p>
 *
 * @author Manik Surtani
 * @see Scope
 * @since 4.0
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
