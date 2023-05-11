package org.infinispan.util;

import org.infinispan.affinity.impl.KeyAffinityServiceImpl;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.internal.CommonsBlockHoundIntegration;
import org.infinispan.container.offheap.OffHeapConcurrentMap;
import org.infinispan.container.offheap.SegmentedBoundedOffHeapDataContainer;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.expiration.impl.ClusterExpirationManager;
import org.infinispan.factories.impl.BasicComponentRegistryImpl;
import org.infinispan.factories.threads.EnhancedQueueExecutorFactory;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistryImpl;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.sifs.TemporaryTable;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.statetransfer.StateTransferLockImpl;
import org.infinispan.topology.ClusterTopologyManagerImpl;
import org.infinispan.topology.LocalTopologyManagerImpl;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;
import org.jgroups.JChannel;
import org.jgroups.blocks.cs.TcpConnection;
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.TimeScheduler3;
import org.kohsuke.MetaInfServices;

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
         // This acquires the lruLock and also OffHeapConcurrentMap stampedLocks when processing eviction
         builder.allowBlockingCallsInside(SegmentedBoundedOffHeapDataContainer.class.getName(), "ensureSize");
         CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, StateTransferLockImpl.class);

         // LimitedExecutor just submits a task to another thread pool
         builder.allowBlockingCallsInside(LimitedExecutor.class.getName(), "acquireLock");
         // This invokes the actual runnable - we have to make sure it doesn't block as normal
         builder.disallowBlockingCallsInside(LimitedExecutor.class.getName(), "actualRun");

         // This method by design will never block; It may block very shortly if another thread is removing or adding
         // to the queue, but it will never block for an extended period by design as there will always be room
         builder.allowBlockingCallsInside(ClusterExpirationManager.class.getName(), "addStageToPermits");

         // This shouldn't block long when held - but it is a write lock which can be delayed
         builder.allowBlockingCallsInside(KeyAffinityServiceImpl.class.getName(), "handleViewChange");

         builder.allowBlockingCallsInside(TransactionTable.class.getName(), "calculateMinTopologyId");

         builder.allowBlockingCallsInside(ClusterTopologyManagerImpl.class.getName(), "acquireUpdateLock");

         builder.allowBlockingCallsInside(PersistenceManagerImpl.class.getName(), "acquireReadLock");

         builder.allowBlockingCallsInside(JGroupsTransport.class.getName(), "withView");
      }
      // This invokes the actual runnable - we have to make sure it doesn't block as normal
      builder.disallowBlockingCallsInside(LimitedExecutor.class.getName(), "actualRun");

      // If shutting down a cache manager - don't worry if blocking
      builder.allowBlockingCallsInside(DefaultCacheManager.class.getName(), "stop");

      // The blocking iterator locks to signal at the end - ignore (we can't reference class object as it is internal)
      builder.allowBlockingCallsInside("io.reactivex.rxjava3.internal.operators.flowable.BlockingFlowableIterable" + "$BlockingFlowableIterator", "signalConsumer");

      // Loading up the EnhancedQueueExecutor class loads org.jboss.threads.Version that reads a file to determine version
      builder.allowBlockingCallsInside(EnhancedQueueExecutorFactory.class.getName(), "createExecutor");

      // Reads from a file during initialization which is during store startup
      builder.allowBlockingCallsInside("org.infinispan.persistence.sifs.Index", "checkForExistingIndexSizeFile");

      methodsToBeRemoved(builder);

      questionableMethodsAllowedToBlock(builder);

      jgroups(builder);
   }

   private void jgroups(BlockHound.Builder builder) {
      // Just ignore jgroups for now and assume it is non blocking
      builder.allowBlockingCallsInside(JChannel.class.getName(), "send");
      builder.allowBlockingCallsInside(ForkChannel.class.getName(), "send");
      // Sometimes JGroups sends messages or does other blocking stuff without going through the channel
      builder.allowBlockingCallsInside(TcpConnection.class.getName(), "connect");
      builder.allowBlockingCallsInside(TcpConnection.class.getName(), "send");
      builder.allowBlockingCallsInside(TcpConnection.class.getName() + "$Receiver", "run");
      // Blocking internals
      builder.allowBlockingCallsInside(TimeScheduler3.class.getName(), "add");
      builder.allowBlockingCallsInside(GMS.class.getName(), "process");
      builder.allowBlockingCallsInside(UNICAST3.class.getName(), "triggerXmit");
   }

   /**
    * Various methods that need to be removed as they are essentially bugs. Please ensure that a JIRA is created and
    * referenced here for any such method
    * @param builder the block hound builder to register methods
    */
   private static void methodsToBeRemoved(BlockHound.Builder builder) {
      // The internal map only supports local mode - we need to replace with Caffeine
      // https://issues.redhat.com/browse/ISPN-11272
      builder.allowBlockingCallsInside(RecoveryManagerImpl.class.getName(), "registerInDoubtTransaction");

      // SoftIndexFileStore locks and awaits on write if there is a concurrent compaction
      // https://issues.redhat.com/browse/ISPN-13799
      builder.allowBlockingCallsInside(TemporaryTable.class.getName(), "set");
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
      // This happens when a cache is requested while it is still starting
      // Due to this happening at startup or extremely rarely at runtime, we can ignore it
      // This should be fixed in https://issues.redhat.com/browse/ISPN-11396
      CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, BasicComponentRegistryImpl.class);
      try {
         CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, Class.forName(BasicComponentRegistryImpl.class.getName() + "$ComponentWrapper"));
      } catch (ClassNotFoundException e) {
         throw new CacheException(e);
      }

      // Can wait on lock
      builder.allowBlockingCallsInside(ClusterTopologyManagerImpl.class.getName(), "updateState");

      // These methods to I/O via GlobalStateManager
      builder.allowBlockingCallsInside(ClusterTopologyManagerImpl.class.getName(), "initCacheStatusIfAbsent");
      builder.allowBlockingCallsInside(LocalTopologyManagerImpl.class.getName(), "writeCHState");
      builder.allowBlockingCallsInside(LocalTopologyManagerImpl.class.getName(), "deleteCHState");
      builder.allowBlockingCallsInside(LocalTopologyManagerImpl.class.getName(), "getNumberMembersFromState");

      // This can block if there is a store otherwise it won't block
      builder.allowBlockingCallsInside(CacheMgmtInterceptor.class.getName(), "getNumberOfEntries");

      // Unfortunately retrieving the protobuf schema reads from a separately generated file - We hope this can be changed
      // so instead the generated context initializer can just store the schema as a String.
      builder.allowBlockingCallsInside(SerializationContextRegistryImpl.class.getName(), "register");
   }
}
