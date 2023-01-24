package org.infinispan.security.actions;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.EmptyRaftManager;
import org.infinispan.remoting.transport.raft.RaftManager;

final class GetRaftManagerAction extends AbstractEmbeddedCacheManagerAction<RaftManager> {

   public GetRaftManagerAction(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public RaftManager get() {
      Transport transport = GlobalComponentRegistry.componentOf(cacheManager, Transport.class);
      return transport == null ? EmptyRaftManager.INSTANCE : transport.raftManager();
   }
}
