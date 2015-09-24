package org.infinispan.security;

import org.infinispan.Cache;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * GlobalSecurityManager.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@Scope(Scopes.GLOBAL)
public interface GlobalSecurityManager {

   /**
    * Returns the global ACL cache
    */
   Cache<?, ?> globalACLCache();

   /**
    * Flushes the ACL cache for this node
    */
   void flushGlobalACLCache();

}
