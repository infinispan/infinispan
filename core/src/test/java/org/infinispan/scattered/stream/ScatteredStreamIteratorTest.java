package org.infinispan.scattered.stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.scattered.ScatteredStateProvider;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.stream.DistributedStreamIteratorTest;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Answer;
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
      // Scattered cache doesn't use StateProvider.startOutboundTransfer,
      // so we block ScatteredStateProviderImpl.startKeysTransferInstead
      StateProvider realObject = TestingUtil.extractComponent(cache, StateProvider.class);
      Answer<?> forwardedAnswer = AdditionalAnswers.delegatesTo(realObject);
      ScatteredStateProvider mock = mock(ScatteredStateProvider.class, withSettings().defaultAnswer(forwardedAnswer));
      // TODO Replace with Mocks.blockingAnswer() once ISPN-10864 is fixed
      doAnswer(invocation -> {
         checkPoint.trigger(Mocks.BEFORE_INVOCATION);
         // Scattered cache iteration blocks and waits for state transfer to fetch the values
         // if a segment is owned in the pending CH, so we can't block forever
         // Instead we just block for 2 seconds to verify ISPN-10984
         checkPoint.peek(2, TimeUnit.SECONDS, Mocks.BEFORE_RELEASE);
         try {
            return forwardedAnswer.answer(invocation);
         } finally {
            checkPoint.trigger(Mocks.AFTER_INVOCATION);
            checkPoint.awaitStrict(Mocks.AFTER_RELEASE, 20, TimeUnit.SECONDS);
         }
      }).when(mock)
        .startKeysTransfer(any(), any());
      TestingUtil.replaceComponent(cache, StateProvider.class, mock, true);
   }
}
