package org.infinispan.security;

import java.util.Map;
import java.util.concurrent.CompletionStage;

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
   <K, V> Map<K, V> globalACLCache();

   /**
    * Flushes the ACL cache for this node
    * @return
    */
   CompletionStage<Void> flushGlobalACLCache();

}
