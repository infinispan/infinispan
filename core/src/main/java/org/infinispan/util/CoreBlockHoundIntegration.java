package org.infinispan.util;

import org.infinispan.affinity.impl.KeyAffinityServiceImpl;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.commons.internal.CommonsBlockHoundIntegration;
import org.infinispan.container.offheap.OffHeapConcurrentMap;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.expiration.impl.ClusterExpirationManager;
import org.infinispan.factories.impl.BasicComponentRegistryImpl;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.statetransfer.StateTransferLockImpl;
import org.infinispan.topology.ClusterTopologyManagerImpl;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;
import org.jgroups.JChannel;
import org.kohsuke.MetaInfServices;

import io.reactivex.internal.operators.flowable.BlockingFlowableIterable;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@SuppressWarnings("unused")
@MetaInfServices
public class CoreBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      registerBlockingMethods(builder);

      // Block designates methods that should only hold a lock very briefly
      {
         CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, OffHeapConcurrentMap.class);
         CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, StateTransferLockImpl.class);
         CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, LimitedExecutor.class);

         // This method by design will never block; It may block very shortly if another thread is removing or adding
         // to the queue, but it will never block for an extended period by design as there will always be room
         builder.allowBlockingCallsInside(ClusterExpirationManager.class.getName(), "addStageToPermits");

         // This shouldn't block long when held - but it is a write lock which can be delayed
         builder.allowBlockingCallsInside(KeyAffinityServiceImpl.class.getName(), "handleViewChange");
      }
      // This invokes the actual runnable - we have to make sure it doesn't block as normal
      builder.disallowBlockingCallsInside(LimitedExecutor.class.getName(), "actualRun");

      // If shutting down a cache manager - don't worry if blocking
      builder.allowBlockingCallsInside(DefaultCacheManager.class.getName(), "stop");

      // The blocking iterator locks to signal at the end - ignore
      builder.allowBlockingCallsInside(BlockingFlowableIterable.class.getName() + "$BlockingFlowableIterator", "signalConsumer");

      methodsToBeRemoved(builder);

      questionableMethodsAllowedToBlock(builder);

      // Just ignore jgroups for now and assume it is non blocking
      builder.allowBlockingCallsInside(JChannel.class.getName(), "send");
   }

   private static void methodsToBeRemoved(BlockHound.Builder builder) {
      // The internal map only supports local mode - we need to replace with Caffeine
      // https://issues.redhat.com/browse/ISPN-11272
      builder.allowBlockingCallsInside(RecoveryManagerImpl.class.getName(), "registerInDoubtTransaction");
   }

   private static void registerBlockingMethods(BlockHound.Builder builder) {
      builder.markAsBlocking(CacheImpl.class, "size", "()I");
      builder.markAsBlocking(CacheImpl.class, "size", "(J)I");
      builder.markAsBlocking(CacheImpl.class, "containsKey", "(Ljava/lang/Object;)Z");
      builder.markAsBlocking(CacheImpl.class, "get", "(Ljava/lang/Object;)Ljava/lang/Object;");
      builder.markAsBlocking(CacheImpl.class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

      // Distributed streams are blocking!
      builder.markAsBlocking("org.infinispan.interceptors.distribution.DistributionBulkInterceptor$BackingEntrySet", "stream", "()Lorg/infinispan/CacheStream;");
      builder.markAsBlocking("org.infinispan.interceptors.distribution.DistributionBulkInterceptor$BackingEntrySet", "parallelStream", "()Lorg/infinispan/CacheStream;");
   }

   private static void questionableMethodsAllowedToBlock(BlockHound.Builder builder) {
      // Component registry has a lock to protect its state - is short lived lock
      builder.allowBlockingCallsInside(BasicComponentRegistryImpl.class.getName(), "prepareWrapperChange");

      // This one should probably not be allowed - it is waiting for another component to start
      // TODO: This might actually be a bug in the rewiring logic. (shows in StateTransferOverwritingValueTest)
      builder.allowBlockingCallsInside(BasicComponentRegistryImpl.class.getName(), "awaitWrapperState");

      // This method calls initCacheStatusIfAbsent which can invoke readScopedState which reads scope from a file that
      // can block the current thread while doing I/O
      builder.allowBlockingCallsInside(ClusterTopologyManagerImpl.class.getName(), "prepareJoin");
      builder.allowBlockingCallsInside(ClusterTopologyManagerImpl.class.getName(), "updateClusterState");
   }
}
