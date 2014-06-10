package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.util.concurrent.WithinThreadExecutor;

/**
 * GetDefaultExecutorServiceAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetDefaultExecutorServiceAction implements PrivilegedAction<DefaultExecutorService>{

   private final Cache<?, ?> cache;

   public GetDefaultExecutorServiceAction(Cache<?, ?> cache) {
      this.cache = cache;
   }

   @Override
   public DefaultExecutorService run() {
      return new DefaultExecutorService(cache, new WithinThreadExecutor());
   }

}
