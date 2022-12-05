package org.infinispan.query.remote.impl.filter;

import static org.infinispan.security.Security.doPrivileged;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.impl.GetSerializationContextAction;

/**
 * SecurityActions for the org.infinispan.query.remote.impl.filter package.
 *
 * Do not move and do not change class and method visibility!
 *
 * @author Dan Berindei
 * @since 10.0
 */
final class SecurityActions {

   static SerializationContext getSerializationContext(EmbeddedCacheManager cacheManager) {
      return doPrivileged(new GetSerializationContextAction(cacheManager));
   }
}
