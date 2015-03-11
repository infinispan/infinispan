package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.distribution.DistributionTestHelper.hasOwners;
import static org.testng.AssertJUnit.*;

/**
 * Tests if the entry version is lost during the state transfer in which the primary owner changes.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "statetransfer.WriteSkewDuringStateTransferTest", singleThreaded = true)
public class WriteSkewDuringStateTransferTest extends MultipleCacheManagersTest {

   private final List<BlockingLocalTopologyManager> topologyManagerList =
         Collections.synchronizedList(new ArrayList<BlockingLocalTopologyManager>(4));

   @AfterMethod(alwaysRun = true)
   public final void unblockAll() {
      //keep track of all controlled components. In case of failure, we need to unblock all otherwise we have to wait
      //long time until the test is able to stop all cache managers.
      for (BlockingLocalTopologyManager topologyManager : topologyManagerList) {
         topologyManager.stopBlockingAll();
      }
      topologyManagerList.clear();
   }

   /*
   Replicated TX cache with WSC, A, B are in cluster, C is joining
   0. The current CH already contains A and B as owners, C is joining (is not primary owner of anything yet).
B is primary owner of K=V.
   1. A sends PrepareCommand to B and C with put(K, V) (V is null on all nodes); //A has already received the
rebalance_start
   2. C receives PrepareCommand and responds with no versions (it is not primary owner); //C has already received the
rebalance_start
   3. topology changes on B - primary ownership of K is transferred to C; //B has already received the ch_update
   4. B receives PrepareCommand, responds without K's version (it is not primary)
   5. B forwards the Prepare to C as it sees that the command has lower topology ID
   6. C responds to B's prepare with version of K; //at this point, C has received the ch_update
   7. K version is not added to B's response, B responds to A
   8. A finds out that topology has changed, forwards prepare to C; //A has received the ch_update
   9. C responds to C's prepare with version of K
   10. A receives C's response, but the versions are not added to transaction
   11. A sends out CommitCommand missing version of K
   12. all nodes record K=V without version as usual ImmortalCacheEntry
   */

   /**
    * See description above or ISPN-3738
    */
   public void testVersionsAfterStateTransfer() throws Exception {
      assertClusterSize("Wrong cluster size", 2);
      final Object key = "key1";
      assertKeyOwnership(key, cache(1), cache(0));
      final int currentTopologyId = currentTopologyId(cache(0));

      final ControlledRpcManager nodeARpcManager = replaceRpcManager(cache(0));
      final NodeController nodeAController = setNodeControllerIn(cache(0));
      setInitialPhaseForNodeA(nodeAController, currentTopologyId);
      final NodeController nodeBController = setNodeControllerIn(cache(1));
      setInitialPhaseForNodeB(nodeBController, currentTopologyId);
      final NewNode nodeC = addNode(currentTopologyId);

      //node A thinks that node B is the primary owner. Node B is blocking the prepare command until it thinks that
      //node C is the primary owner
      nodeAController.topologyManager.waitToBlock(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);
      nodeARpcManager.blockAfter(VersionedPrepareCommand.class);
      //node C thinks that node B is the primary owner.
      //nodeC.controller.topologyManager.waitToBlock(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);

      //after this waiting phase, node A thinks that node B is the primary owner, node B thinks that node C is the
      // primary owner and node C thinks that node B is the primary owner
      //lets execute the transaction...

      Future<Object> tx = executeTransaction(cache(0), key);

      //it waits until all nodes has replied. then, we change the topology ID and let it collect the responses.
      nodeARpcManager.waitForCommandToBlock();
      nodeAController.topologyManager.stopBlocking(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);
      awaitForTopology(currentTopologyId + 3, cache(0));

      nodeARpcManager.stopBlocking();
      assertNull("Wrong put() return value.", tx.get());

      nodeAController.topologyManager.stopBlockingAll();
      nodeBController.topologyManager.stopBlockingAll();
      nodeC.controller.topologyManager.stopBlockingAll();

      nodeC.joinerFuture.get();

      awaitForTopology(currentTopologyId + 3, cache(0));
      awaitForTopology(currentTopologyId + 3, cache(1));
      awaitForTopology(currentTopologyId + 3, cache(2));

      assertKeyVersionInDataContainer(key, cache(1), cache(2));
      cache(0).put(key, "v2");
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, configuration());
   }

   private void assertKeyVersionInDataContainer(Object key, Cache... owners) {
      for (Cache cache : owners) {
         DataContainer dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
         InternalCacheEntry entry = dataContainer.get(key);
         assertNotNull("Entry cannot be null in " + address(cache) + ".", entry);
         assertNotNull("Version cannot be null.", entry.getMetadata().version());
      }
   }

   private ControlledRpcManager replaceRpcManager(Cache cache) {
      RpcManager manager = TestingUtil.extractComponent(cache, RpcManager.class);
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(manager);
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
      //rpcManagerList.add(controlledRpcManager);
      return controlledRpcManager;
   }

   private void awaitForTopology(final int expectedTopologyId, final Cache cache) {
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return expectedTopologyId == currentTopologyId(cache);
         }
      });
   }

   private int currentTopologyId(Cache cache) {
      return TestingUtil.extractComponent(cache, StateTransferManager.class).getCacheTopology().getTopologyId();
   }

   private Future<Object> executeTransaction(final Cache<Object, Object> cache, final Object key) {
      return fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            return TestingUtil.withTx(cache.getAdvancedCache().getTransactionManager(), new Callable<Object>() {
               @Override
               public Object call() throws Exception {
                  return cache.put(key, "value");
               }
            });
         }
      });
   }

   private NewNode addNode(final int currentTopologyId) {
      final NewNode newNode = new NewNode();
      ConfigurationBuilder builder = configuration();
      newNode.controller = new NodeController();
      newNode.controller.interceptor = new ControlledCommandInterceptor();
      builder.customInterceptors().addInterceptor().index(0).interceptor(newNode.controller.interceptor);
      EmbeddedCacheManager embeddedCacheManager = addClusterEnabledCacheManager(builder);
      newNode.controller.topologyManager = replaceTopologyManager(embeddedCacheManager);
      newNode.controller.interceptor.addAction(new Action() {
         @Override
         public boolean isApplicable(InvocationContext context, VisitableCommand command) {
            return !context.isOriginLocal() && command instanceof PrepareCommand;
         }

         @Override
         public void before(InvocationContext context, VisitableCommand command, Cache cache) {
            log.tracef("Before: command=%s. origin=%s", command, context.getOrigin());
            if (context.getOrigin().equals(address(cache(1)))) {
               //from node B, i.e, it is forwarded. it needs to wait until the topology changes
               try {
                  cache.getAdvancedCache().getComponentRegistry().getStateTransferLock().waitForTopology(currentTopologyId + 2,
                                                                                                         10, TimeUnit.SECONDS);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               }
            }
         }

         @Override
         public void after(InvocationContext context, VisitableCommand command, Cache cache) {
            log.tracef("After: command=%s. origin=%s", command, context.getOrigin());
            if (context.getOrigin().equals(address(cache(0)))) {
               newNode.controller.topologyManager.stopBlocking(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);
            }
         }
      });

      newNode.controller.topologyManager.startBlocking(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);
      newNode.joinerFuture = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            waitForClusterToForm();
            return null;
         }
      });
      return newNode;
   }

   private ConfigurationBuilder configuration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      builder.clustering()
            .stateTransfer().fetchInMemoryState(true)
            .hash().numSegments(1).numOwners(3).consistentHashFactory(new ConsistentHashFactoryImpl());
      builder.locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(true);
      builder.versioning()
            .enable()
            .scheme(VersioningScheme.SIMPLE);
      return builder;
   }

   private void assertKeyOwnership(Object key, Cache primaryOwner, Cache... backupOwners) {
      assertTrue("Wrong ownership for " + key + ".", hasOwners(key, primaryOwner, backupOwners));
   }

   private BlockingLocalTopologyManager replaceTopologyManager(CacheContainer cacheContainer) {
      BlockingLocalTopologyManager localTopologyManager = BlockingLocalTopologyManager.replaceTopologyManager(cacheContainer);
      topologyManagerList.add(localTopologyManager);
      return localTopologyManager;
   }

   private static NodeController setNodeControllerIn(Cache<Object, Object> cache) {
      NodeController nodeController = new NodeController();
      nodeController.interceptor = new ControlledCommandInterceptor(cache);
      nodeController.topologyManager = BlockingLocalTopologyManager.replaceTopologyManager(cache.getCacheManager());
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
         public void before(InvocationContext context, VisitableCommand command, Cache cache) {
            try {
               cache.getAdvancedCache().getComponentRegistry().getStateTransferLock().waitForTopology(currentTopology + 1,
                                                                                                      10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }

         @Override
         public void after(InvocationContext context, VisitableCommand command, Cache cache) {
            //no-op
         }
      });
      nodeA.topologyManager.startBlocking(BlockingLocalTopologyManager.LatchType.WRITE_CH_UPDATE);
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
         public void before(InvocationContext context, VisitableCommand command, Cache cache) {
            try {
               cache.getAdvancedCache().getComponentRegistry().getStateTransferLock().waitForTopology(currentTopology + 2,
                                                                                                      10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }

         @Override
         public void after(InvocationContext context, VisitableCommand command, Cache cache) {
            //no-op
         }
      });
   }

   public interface Action {

      public boolean isApplicable(InvocationContext context, VisitableCommand command);

      public void before(InvocationContext context, VisitableCommand command, Cache cache);

      public void after(InvocationContext context, VisitableCommand command, Cache cache);

   }

   public static class ConsistentHashFactoryImpl extends BaseControlledConsistentHashFactory {

      public ConsistentHashFactoryImpl() {
         super(1);
      }

      @Override
      protected final List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex) {
         assertEquals("Wrong number of owners", 3, numberOfOwners);
         //the primary owner is the last member.
         final List<Address> owners = new ArrayList<Address>(3);
         owners.add(members.get(members.size() - 1));
         for (int i = 0; i < members.size() - 1; ++i) {
            owners.add(members.get(i));
         }
         return owners;
      }
   }

   public static class ControlledCommandInterceptor extends BaseCustomInterceptor {

      private final List<Action> actionList;

      public ControlledCommandInterceptor(Cache<Object, Object> cache) {
         actionList = new ArrayList<Action>(3);
         this.cache = cache;
         this.cacheConfiguration = cache.getCacheConfiguration();
         this.embeddedCacheManager = cache.getCacheManager();
         cache.getAdvancedCache().addInterceptor(this, 0);
      }

      public ControlledCommandInterceptor() {
         actionList = new ArrayList<Action>(3);
      }

      public void addAction(Action action) {
         actionList.add(action);
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         List<Action> actions = extractActions(ctx, command);
         if (actions.isEmpty()) {
            return invokeNextInterceptor(ctx, command);
         }
         for (Action action : actions) {
            action.before(ctx, command, cache);
         }
         Object retVal = invokeNextInterceptor(ctx, command);
         for (Action action : actions) {
            action.after(ctx, command, cache);
         }
         return retVal;
      }

      private List<Action> extractActions(InvocationContext context, VisitableCommand command) {
         if (actionList.isEmpty()) {
            return Collections.emptyList();
         }
         List<Action> actions = new ArrayList<Action>(actionList.size());
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
      NodeController controller;
   }
}
