package org.infinispan.statetransfer;

import static org.infinispan.test.TestingUtil.wrapComponent;
import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.AbstractDelegatingRpcManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Test for https://issues.jboss.org/browse/ISPN-6047
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
@Test(groups = "functional", testName = "statetransfer.TxReplay3Test")
public class TxReplay3Test extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(TxReplay3Test.class);

   private static final String VALUE_1 = "v1";
   private static final String VALUE_2 = "v2";

   private static final String TX1_LOCKED = "tx1:acquired_lock";
   private static final String TX1_UNSURE = "tx1:unsure_response";
   private static final String TX2_PENDING = "tx2:waiting_tx1";
   private static final String MAIN_ADVANCE = "main:advance";
   private static final String JOIN_NEW_NODE = "join:add_new_node";

   public void testReplay() throws Exception {
      final Object key = new MagicKey("TxReplay3Test", cache(0));
      final StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("tx1", TX1_LOCKED, TX1_UNSURE);
      sequencer.logicalThread("tx2", TX2_PENDING);
      sequencer.logicalThread("join", JOIN_NEW_NODE);
      sequencer.logicalThread("main", MAIN_ADVANCE);
      sequencer.order(TX1_LOCKED, MAIN_ADVANCE, TX2_PENDING, JOIN_NEW_NODE, TX1_UNSURE);


      wrapComponent(cache(1), RpcManager.class,
                    (wrapOn, current) -> new UnsureResponseRpcManager(current, sequencer), true);
      Handler handler = wrapInboundInvocationHandler(cache(0), current -> new Handler(current, sequencer));
      handler.setOrigin(address(cache(2)));

      Future<Void> tx1 = fork(() -> {
         cache(1).put(key, VALUE_1);
         return null;
      });

      sequencer.advance(MAIN_ADVANCE);

      Future<Void> tx2 = fork(() -> {
         cache(2).put(key, VALUE_2);
         return null;
      });

      sequencer.enter(JOIN_NEW_NODE);
      addClusterEnabledCacheManager(ControlledConsistentHashFactory.SCI.INSTANCE, config()).getCache();
      waitForClusterToForm();
      sequencer.exit(JOIN_NEW_NODE);


      tx1.get(30, TimeUnit.SECONDS);
      tx2.get(30, TimeUnit.SECONDS);

      assertEquals(VALUE_2, cache(0).get(key));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, ControlledConsistentHashFactory.SCI.INSTANCE, config());
   }

   private static ConfigurationBuilder config() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction()
            .useSynchronization(false)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .recovery().disable();
      builder.locking().lockAcquisitionTimeout(1, TimeUnit.MINUTES).isolationLevel(IsolationLevel.READ_COMMITTED);
      builder.clustering()
            .remoteTimeout(1, TimeUnit.MINUTES)
            .hash().numOwners(1).numSegments(1)
            .consistentHashFactory(new ControlledConsistentHashFactory.Default(0))
            .stateTransfer().fetchInMemoryState(false);
      return builder;
   }

   private static class UnsureResponseRpcManager extends AbstractDelegatingRpcManager {

      private final StateSequencer sequencer;
      private volatile boolean triggered = false;

      public UnsureResponseRpcManager(RpcManager realOne, StateSequencer sequencer) {
         super(realOne);
         this.sequencer = sequencer;
      }

      @Override
      protected <T> CompletionStage<T> performRequest(Collection<Address> targets, ReplicableCommand command,
                                                      ResponseCollector<T> collector,
                                                      Function<ResponseCollector<T>, CompletionStage<T>> invoker,
                                                      RpcOptions rpcOptions) {
         return super.performRequest(targets, command, collector, invoker, rpcOptions)
            .thenApply(result -> {
               log.debugf("After invoke remotely %s. Responses=%s", command, result);
               if (triggered || !(command instanceof PrepareCommand))
                  return result;

               log.debugf("Triggering %s and %s", TX1_LOCKED, TX1_UNSURE);
               triggered = true;
               try {
                  sequencer.advance(TX1_LOCKED);
                  sequencer.advance(TX1_UNSURE);
               } catch (TimeoutException | InterruptedException e) {
                  throw new CacheException(e);
               }
               Map<Address, Response> newResult = new HashMap<>();
               ((Map<Address, Response>) result).forEach((address, response) -> newResult.put(address, UnsureResponse.INSTANCE));
               log.debugf("After invoke remotely %s. New Responses=%s", command, newResult);
               return (T) newResult;
            });
      }
   }

   private static class Handler extends AbstractDelegatingHandler {

      private final StateSequencer sequencer;
      private volatile boolean triggered = false;
      private volatile Address origin;

      public Handler(PerCacheInboundInvocationHandler delegate, StateSequencer sequencer) {
         super(delegate);
         this.sequencer = sequencer;
      }

      public void setOrigin(Address origin) {
         this.origin = origin;
      }

      @Override
      protected boolean beforeHandle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         log.debugf("Before invoking %s. expected origin=%s", command, origin);
         return super.beforeHandle(command, reply, order);
      }

      @Override
      protected void afterHandle(CacheRpcCommand command, DeliverOrder order, boolean delegated) {
         super.afterHandle(command, order, delegated);
         log.debugf("After invoking %s. expected origin=%s", command, origin);
         if (!triggered && command instanceof PrepareCommand && command.getOrigin().equals(origin)) {
            log.debugf("Triggering %s.", TX2_PENDING);
            triggered = true;
            try {
               sequencer.advance(TX2_PENDING);
            } catch (TimeoutException | InterruptedException e) {
               throw new CacheException(e);
            }
         }
      }
   }
}
