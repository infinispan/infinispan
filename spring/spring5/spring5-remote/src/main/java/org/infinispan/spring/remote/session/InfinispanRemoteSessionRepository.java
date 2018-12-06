package org.infinispan.spring.remote.session;


import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;
import org.springframework.core.task.TaskExecutor;

/**
 * Session Repository for Infinispan in client/server mode.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public class InfinispanRemoteSessionRepository extends AbstractInfinispanSessionRepository {

   /**
    * Creates new repository based on {@link SpringCache}
    *
    * @param cache Cache which shall be used for session repository.
    */
   public InfinispanRemoteSessionRepository(SpringCache cache, TaskExecutor taskExecutor) {
      super(cache, new RemoteApplicationPublishedBridge(cache, taskExecutor));
   }
}
