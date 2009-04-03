package org.horizon.factories.scopes;

/**
 * The different scopes that can be declared for a component in the cache system.  If components are not bounded to a
 * specific scope explicity, then it defaults to the {@link #NAMED_CACHE} scope.
 *
 * @author Manik Surtani
 * @see Scope
 * @since 4.0
 */
public enum Scopes {
   /**
    * Components bounded to this scope can only be created by a {@link org.horizon.manager.DefaultCacheManager} and exist in
    * the {@link org.horizon.manager.DefaultCacheManager}'s {@link org.horizon.factories.ComponentRegistry}.
    */
   GLOBAL,

   /**
    * Components bounded to this scope can only be created by a {@link org.horizon.Cache} and exist in the {@link
    * org.horizon.Cache}'s {@link org.horizon.factories.ComponentRegistry}.
    */
   NAMED_CACHE;

   public static Scopes getDefaultScope() {
      return NAMED_CACHE;
   }
}
