package org.infinispan.interceptors;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;

/**
 * Base class for an interceptor in the new sequential invocation chain.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public abstract class BaseAsyncInterceptor implements AsyncInterceptor {
   protected Configuration cacheConfiguration;

   @Inject
   public void inject(Configuration cacheConfiguration) {
      this.cacheConfiguration = cacheConfiguration;
   }
}
