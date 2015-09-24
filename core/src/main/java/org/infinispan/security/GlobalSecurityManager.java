package org.infinispan.security;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.security.impl.SubjectACL;

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
   Cache<Subject, SubjectACL> getGlobalACLCache();

   /**
    * Flushes the ACL cache for this node
    */
   void flushACLCache();

}
