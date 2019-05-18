package org.infinispan.query.remote.impl.indexing;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.impl.GetSerializationContextAction;
import org.infinispan.security.Security;

/**
 * SecurityActions for the org.infinispan.query.remote.impl.indexing package.
 *
 * Do not move and do not change class and method visibility!
 *
 * @author Dan Berindei
 * @since 10.0
 */
final class SecurityActions {

   private SecurityActions() {
   }

   private static <T> T doPrivileged(PrivilegedAction<T> action) {
      return System.getSecurityManager() != null ?
            AccessController.doPrivileged(action) : Security.doPrivileged(action);
   }

   static SerializationContext getSerializationContext(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetSerializationContextAction(cacheManager));
   }
}
