package org.infinispan.scattered.stream;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.statetransfer.ScatteredStateGetKeysCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.stream.DistributedStreamIteratorTest;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = {"functional", "smoke"}, testName = "iteration.ScatteredStreamIteratorTest")
public class ScatteredStreamIteratorTest extends DistributedStreamIteratorTest {
   public ScatteredStreamIteratorTest() {
      super(false, CacheMode.SCATTERED_SYNC);
   }

   @Override
   public void testNodeLeavesWhileIteratingOverContainerCausingRehashToLoseValues() {
      // Test is ignored until https://issues.jboss.org/browse/ISPN-10864 can be fixed
   }

   @Override
   public void waitUntilProcessingResults() {
      // Test is ignored until https://issues.jboss.org/browse/ISPN-10864 can be fixed
   }

   @Override
   protected <K> void blockStateTransfer(Cache<?, ?> cache, CheckPoint checkPoint) {
      // TODO Replace with Mocks.blockInboundCacheRpcCommand() once ISPN-10864 is fixed
      Executor executor = extractGlobalComponent(cache.getCacheManager(), ExecutorService.class,
                                                 KnownComponentNames.NON_BLOCKING_EXECUTOR);
      TestingUtil.wrapInboundInvocationHandler(cache, handler -> new AbstractDelegatingHandler(handler) {
         @Override
         public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
            if (!(command instanceof ScatteredStateGetKeysCommand)) {
               delegate.handle(command, reply, order);
               return;
            }

            checkPoint.trigger(Mocks.BEFORE_INVOCATION);
            // Scattered cache iteration blocks and waits for state transfer to fetch the values
            // if a segment is owned in the pending CH, so we can't block forever
            // Instead we just block for 2 seconds to verify ISPN-10984
            checkPoint.future(Mocks.BEFORE_RELEASE, 2, TimeUnit.SECONDS, executor)
                      .whenComplete((ignored1, ignored2) -> delegate.handle(command, reply, order))
                      .thenCompose(ignored -> {
                         checkPoint.trigger(Mocks.AFTER_INVOCATION);
                         return checkPoint.future(Mocks.AFTER_RELEASE, 20, TimeUnit.SECONDS, executor);
                      });
         }
      });
   }
}
