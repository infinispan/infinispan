package org.infinispan.interceptors.sequential;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.SequentialInterceptor;

/**
 * Base class for an interceptor in the new sequential invocation chain.
 *
 * @author Dan Berindei
 * @since 8.1
 */
public abstract class
BaseSequentialInterceptor implements SequentialInterceptor {
   protected Configuration cacheConfiguration;

   @Inject
   public void inject(Configuration cacheConfiguration) {
      this.cacheConfiguration = cacheConfiguration;
   }
}
