package org.infinispan.factories.scopes;

/**
 * The different scopes that can be declared for a component in the cache system.  If components are not bounded to a
 * specific scope explicitly, then it defaults to the {@link #NAMED_CACHE} scope.
 *
 * @author Manik Surtani
 * @see Scope
 * @since 4.0
 */
public enum Scopes {
   /**
    * Components bounded to this scope can only be created by a {@link org.infinispan.manager.DefaultCacheManager} and exist in
    * the {@link org.infinispan.manager.DefaultCacheManager}'s {@link org.infinispan.factories.ComponentRegistry}.
    */
   GLOBAL,

   /**
    * Components bounded to this scope can only be created by a {@link org.infinispan.Cache} and exist in the {@link
    * org.infinispan.Cache}'s {@link org.infinispan.factories.ComponentRegistry}.
    */
   NAMED_CACHE;

   public static Scopes getDefaultScope() {
      return NAMED_CACHE;
   }
}
