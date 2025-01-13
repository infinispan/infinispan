package org.infinispan.statetransfer;

import static org.infinispan.distribution.DistributionTestHelper.hasOwners;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.test.TestingUtil.withTx;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.infinispan.transaction.impl.WriteSkewHelper.versionFromEntry;
import static org.infinispan.util.BlockingLocalTopologyManager.confirmTopologyUpdate;
import static org.infinispan.util.logging.Log.CLUSTER;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.BaseAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests if the entry version is lost during the state transfer in which the primary owner changes.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "statetransfer.WriteSkewDuringStateTransferTest", singleThreaded = true)
public class WriteSkewDuringStateTransferTest extends MultipleCacheManagersTest {

   private final List<BlockingLocalTopologyManager> topologyManagerList =
         Collections.synchronizedList(new ArrayList<>(4));

   @AfterMethod(alwaysRun = true)
   public final void unblockAll() {
      //keep track of all controlled components. In case of failure, we need to unblock all otherwise we have to wait
      //long time until the test is able to stop all cache managers.
      for (BlockingLocalTopologyManager topologyManager : topologyManagerList) {
         topologyManager.stopBlocking();
      }
      topologyManagerList.clear();
   }

   /**
    * See ISPN-3738
    *
    * Replicated TX cache with WSC, A, B are in cluster, C is joining
    * 0. The current CH already contains A and B as owners, C is joining (is not primary owner of anything yet).
    * B is primary owner of K=V.
    * 1. A sends PrepareCommand to B and C with put(K, V) (V is null on all nodes); //A has already received the
    * rebalance_start
    * 2. C receives PrepareCommand and responds with no versions (it is not primary owner); //C has already received the
    * rebalance_start
    * 3. topology changes on B - primary ownership of K is transferred to C; //B has already received the ch_update
    * 4. B receives PrepareCommand, responds without K's version (it is not primary)
    * 5. --B forwards the Prepare to C as it sees that the command has lower topology ID--
    * 6. C responds to B's prepare with version of K; //at this point, C has received the ch_update
    * 7. K version is not added to B's response, B responds to A
    * 8. A finds out that topology has changed, forwards prepare to C; //A has received the ch_update
    * 9. C responds to C's prepare with version of K
    * 10. A receives C's response, but the versions are not added to transaction
    * 11. A sends out CommitCommand missing version of K
    * 12. all nodes record K=V without version as usual ImmortalCacheEntry
    */
   public void testVersionsAfterStateTransfer() throws Exception {
      assertClusterSize("Wrong cluster size", 2);
      final Object key = "key1";
      assertKeyOwnership(key, cache(1), cache(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      final ControlledRpcManager nodeARpcManager = ControlledRpcManager.replaceRpcManager(cache(0));
      final NodeController nodeAController = setNodeControllerIn(cache(0));
      setInitialPhaseForNodeA(nodeAController, currentTopologyId);
      final NodeController nodeBController = setNodeControllerIn(cache(1));
      setInitialPhaseForNodeB(nodeBController, currentTopologyId);
      final NewNode nodeC = addNode(currentTopologyId);

      // Start the rebalance everywhere
      confirmTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL,
                            nodeAController.topologyManager,
                            nodeBController.topologyManager,
                            nodeC.controller.topologyManager);


      // Install the READ_ALL_WRITE_ALL topology on B
      nodeBController.topologyManager.confirmTopologyUpdate(CacheTopology.Phase.READ_ALL_WRITE_ALL);

      Future<Object> tx = executeTransaction(cache(0), key);

      // Wait until all nodes have replied. then, we change the topology ID and let it collect the responses.
      ControlledRpcManager.BlockedResponseMap blockedPrepare =
         nodeARpcManager.expectCommand(VersionedPrepareCommand.class).send().expectAllResponses();
      assertEquals(0, nodeC.commandLatch.getCount());

      nodeAController.topologyManager.confirmTopologyUpdate(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      nodeC.controller.topologyManager.confirmTopologyUpdate(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      confirmTopologyUpdate(CacheTopology.Phase.READ_NEW_WRITE_ALL, nodeAController.topologyManager,
                            nodeBController.topologyManager, nodeC.controller.topologyManager);
      confirmTopologyUpdate(CacheTopology.Phase.NO_REBALANCE, nodeAController.topologyManager,
                            nodeBController.topologyManager, nodeC.controller.topologyManager);
      awaitForTopology(currentTopologyId + 4, cache(0));

      blockedPrepare.receive();

      // Retry the prepare and then commit
      nodeARpcManager.expectCommand(PrepareCommand.class).send().receiveAll();
      nodeARpcManager.expectCommand(CommitCommand.class).send().receiveAll();
      nodeARpcManager.expectCommand(TxCompletionNotificationCommand.class).send();
      assertNull("Wrong put() return value.", tx.get());

      nodeAController.topologyManager.stopBlocking();
      nodeBController.topologyManager.stopBlocking();
      nodeC.controller.topologyManager.stopBlocking();

      nodeC.joinerFuture.get(30, TimeUnit.SECONDS);

      awaitForTopology(currentTopologyId + 4, cache(0));
      awaitForTopology(currentTopologyId + 4, cache(1));
      awaitForTopology(currentTopologyId + 4, cache(2));

      assertKeyVersionInDataContainer(key, cache(1), cache(2));

      nodeARpcManager.stopBlocking();

      cache(0).put(key, "v2");
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, WriteSkewDuringStateTransferSCI.INSTANCE, configuration());
   }

   private void assertKeyVersionInDataContainer(Object key, Cache<?, ?>... owners) {
      for (Cache<?, ?> cache : owners) {
         DataContainer<?, ?> dataContainer = extractComponent(cache, InternalDataContainer.class);
         InternalCacheEntry<?, ?> entry = dataContainer.peek(key);
         assertNotNull("Entry cannot be null in " + address(cache) + ".", entry);
         assertNotNull("Version cannot be null.", versionFromEntry(entry));
      }
   }

   private void awaitForTopology(final int expectedTopologyId, final Cache<?, ?> cache) {
      eventually(() -> expectedTopologyId == currentTopologyId(cache));
   }

   private int currentTopologyId(Cache<?, ?> cache) {
      return cache.getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();
   }

   private Future<Object> executeTransaction(final Cache<Object, Object> cache, final Object key) {
      return fork(() -> withTx(cache.getAdvancedCache().getTransactionManager(), () -> cache.put(key, "value")));
   }

   private NewNode addNode(final int currentTopologyId) {
      final NewNode newNode = new NewNode();
      ConfigurationBuilder builder = configuration();
      newNode.controller = new NodeController();
      newNode.controller.interceptor = new ControlledCommandInterceptor();
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.serialization().addContextInitializer(WriteSkewDuringStateTransferSCI.INSTANCE);
      TestCacheManagerFactory.addInterceptor(global, TestCacheManagerFactory.DEFAULT_CACHE_NAME::equals, newNode.controller.interceptor, TestCacheManagerFactory.InterceptorPosition.FIRST, null);

      EmbeddedCacheManager embeddedCacheManager = createClusteredCacheManager(false, global, builder, new TransportFlags());
      registerCacheManager(embeddedCacheManager);
      newNode.controller.topologyManager = replaceTopologyManager(embeddedCacheManager);

      newNode.controller.interceptor.addAction(new Action() {
         @Override
         public boolean isApplicable(InvocationContext context, VisitableCommand command) {
            return !context.isOriginLocal() && command instanceof PrepareCommand;
         }

         @Override
         public void before(InvocationContext context, VisitableCommand command, Cache<?, ?> cache) {
            log.tracef("Before: command=%s. origin=%s", command, context.getOrigin());
            if (context.getOrigin().equals(address(cache(1)))) {
               //from node B, i.e, it is forwarded. it needs to wait until the topology changes
               try {
                  //noinspection deprecation
                  ComponentRegistry.of(cache).getStateTransferLock().waitForTopology(currentTopologyId + 2,
                                                                                                         10, TimeUnit.SECONDS);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               } catch (TimeoutException e) {
                  throw CLUSTER.failedWaitingForTopology(currentTopologyId + 2);
               }
            }
         }

         @Override
         public void after(InvocationContext context, VisitableCommand command, Cache<?, ?> cache) {
            log.tracef("After: command=%s. origin=%s", command, context.getOrigin());
            if (context.getOrigin().equals(address(0))) {
               newNode.commandLatch.countDown();
            }
         }
      });

      newNode.joinerFuture = fork(() -> {
         // Starts the default cache
         embeddedCacheManager.start();
         return null;
      });
      return newNode;
   }

   private ConfigurationBuilder configuration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder.clustering()
            .stateTransfer().fetchInMemoryState(true)
            .hash().numSegments(1).numOwners(3).consistentHashFactory(new ConsistentHashFactoryImpl());
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      return builder;
   }

   private void assertKeyOwnership(Object key, Cache<?, ?> primaryOwner, Cache<?, ?>... backupOwners) {
      assertTrue("Wrong ownership for " + key + ".", hasOwners(key, primaryOwner, backupOwners));
   }

   private BlockingLocalTopologyManager replaceTopologyManager(EmbeddedCacheManager cacheContainer) {
      BlockingLocalTopologyManager localTopologyManager =
         BlockingLocalTopologyManager.replaceTopologyManagerDefaultCache(cacheContainer);
      topologyManagerList.add(localTopologyManager);
      return localTopologyManager;
   }

   private static NodeController setNodeControllerIn(Cache<Object, Object> cache) {
      NodeController nodeController = new NodeController();
      nodeController.interceptor = new ControlledCommandInterceptor(cache);
      nodeController.topologyManager = BlockingLocalTopologyManager.replaceTopologyManagerDefaultCache(
         cache.getCacheManager());
      return nodeController;
   }

   private static void setInitialPhaseForNodeA(NodeController nodeA, final int currentTopology) {
      //node A initial phase:
      //* Node A sends the prepare for B and C. So, node A will send the prepare after the topologyId+1 is installed.
      nodeA.interceptor.addAction(new Action() {
         @Override
         public boolean isApplicable(InvocationContext context, VisitableCommand command) {
            return context.isOriginLocal() && command instanceof PrepareCommand;
         }

         @Override
         public void before(InvocationContext context, VisitableCommand command, Cache<?, ?> cache) {
            try {
               //noinspection deprecation
               ComponentRegistry.of(cache).getStateTransferLock().waitForTopology(currentTopology + 1, 10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            } catch (TimeoutException e) {
               throw CLUSTER.failedWaitingForTopology(currentTopology + 1);
            }
         }

         @Override
         public void after(InvocationContext context, VisitableCommand command, Cache<?, ?> cache) {
            //no-op
         }
      });
   }

   private static void setInitialPhaseForNodeB(NodeController nodeB, final int currentTopology) {
      //node B initial phase:
      //* Node B receives the prepare after it looses the primary owner to node C
      nodeB.interceptor.addAction(new Action() {
         @Override
         public boolean isApplicable(InvocationContext context, VisitableCommand command) {
            return !context.isOriginLocal() && command instanceof PrepareCommand;
         }

         @Override
         public void before(InvocationContext context, VisitableCommand command, Cache<?, ?> cache) {
            try {
               //noinspection deprecation
               ComponentRegistry.of(cache).getStateTransferLock().waitForTopology(currentTopology + 2,
                                                                                                      10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            } catch (TimeoutException e) {
               throw CLUSTER.failedWaitingForTopology(currentTopology + 2);
            }
         }

         @Override
         public void after(InvocationContext context, VisitableCommand command, Cache<?, ?> cache) {
            //no-op
         }
      });
   }

   public interface Action {

      boolean isApplicable(InvocationContext context, VisitableCommand command);

      void before(InvocationContext context, VisitableCommand command, Cache<?, ?> cache);

      void after(InvocationContext context, VisitableCommand command, Cache<?, ?> cache);

   }

   public static class ConsistentHashFactoryImpl extends BaseControlledConsistentHashFactory.Default {

      ConsistentHashFactoryImpl() {
         super(1);
      }

      @Override
      protected final int[][] assignOwners(int numSegments, List<Address> members) {
         //the primary owner is the last member.
         switch (members.size()) {
            case 1:
               return new int[][]{{0}};
            case 2:
               return new int[][]{{1, 0}};
            default:
               return new int[][]{{members.size() - 1, 0, 1}};
         }
      }
   }

   public static class ControlledCommandInterceptor extends BaseAsyncInterceptor {

      private final List<Action> actionList;
      private Cache<Object, Object> cache;

      public ControlledCommandInterceptor(Cache<Object, Object> cache) {
         actionList = new ArrayList<>(3);
         this.cache = cache;
         this.cacheConfiguration = cache.getCacheConfiguration();
         extractInterceptorChain(cache).addInterceptor(this, 0);
      }

      public ControlledCommandInterceptor() {
         actionList = new ArrayList<>(3);
      }

      public void addAction(Action action) {
         actionList.add(action);
      }

      @Override
      public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
         List<Action> actions = extractActions(ctx, command);
         if (actions.isEmpty()) {
            return invokeNext(ctx, command);
         }
         for (Action action : actions) {
            action.before(ctx, command, cache);
         }
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            for (Action action : actions) {
               action.after(ctx, command, cache);
            }
         });
      }

      private List<Action> extractActions(InvocationContext context, VisitableCommand command) {
         if (actionList.isEmpty()) {
            return Collections.emptyList();
         }
         List<Action> actions = new ArrayList<>(actionList.size());
         for (Action action : actionList) {
            if (action.isApplicable(context, command)) {
               actions.add(action);
            }
         }
         return actions;
      }
   }

   private static class NodeController {
      ControlledCommandInterceptor interceptor;
      BlockingLocalTopologyManager topologyManager;
   }

   private static class NewNode {
      Future<Void> joinerFuture;
      CountDownLatch commandLatch = new CountDownLatch(1);
      NodeController controller;
   }

   @ProtoSchema(
         includeClasses = ConsistentHashFactoryImpl.class,
         schemaFileName = "test.core.WriteSkewDuringStateTransferTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.WriteSkewDuringStateTransferTest",
         service = false
   )
   public interface WriteSkewDuringStateTransferSCI extends SerializationContextInitializer {
      WriteSkewDuringStateTransferSCI INSTANCE = new WriteSkewDuringStateTransferSCIImpl();
   }
}
