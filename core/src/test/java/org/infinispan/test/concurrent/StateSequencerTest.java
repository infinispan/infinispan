package org.infinispan.test.concurrent;

import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.AssertJUnit.assertFalse;

/**
 * {@link StateSequencer} test.
 *
 * @author Dan Berindei
 * @since 7.0
 */
@Test(groups = "unit", testName = "test.concurrent.StateSequencerTest")
public class StateSequencerTest extends AbstractInfinispanTest {
   public void testSingleThread() throws Exception {
      StateSequencer stateSequencer = new StateSequencer();
      stateSequencer.logicalThread("t", "s0", "s1", "s2");

      stateSequencer.advance("s0", 0, SECONDS);
      stateSequencer.advance("s1", 0, SECONDS);
      stateSequencer.advance("s2", 0, SECONDS);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testAdvanceToInvalidState() throws Exception {
      StateSequencer stateSequencer = new StateSequencer();
      stateSequencer.advance("s1", 0, SECONDS);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInvalidDependency() throws Exception {
      StateSequencer stateSequencer = new StateSequencer();
      stateSequencer.logicalThread("t", "s1");
      stateSequencer.order("s0", "s1");
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testInvalidDependency2() throws Exception {
      StateSequencer stateSequencer = new StateSequencer();
      stateSequencer.logicalThread("t", "s1");
      stateSequencer.order("s1", "s2");
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testDependencyCycle() throws Exception {
      StateSequencer stateSequencer = new StateSequencer();
      stateSequencer.logicalThread("t", "s1", "s2", "s3", "s4");
      stateSequencer.order("s4", "s2");
   }

   public void testMultipleThreads() throws Exception {
      final StateSequencer stateSequencer = new StateSequencer();
      stateSequencer.logicalThread("start", "start");
      stateSequencer.logicalThread("t1", "t1:s1").order("start", "t1:s1");
      stateSequencer.logicalThread("t2", "t2:s2").order("start", "t2:s2").order("t1:s1", "t2:s2");
      stateSequencer.logicalThread("stop", "stop").order("t1:s1", "stop").order("t2:s2", "stop");

      Future<Object> future1 = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            stateSequencer.advance("t1:s1", 10, SECONDS);
            return null;
         }
      });

      final AtomicBoolean t2s2Entered = new AtomicBoolean(false);
      Future<Object> future2 = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            stateSequencer.enter("t2:s2", 10, SECONDS);
            t2s2Entered.set(true);
            stateSequencer.exit("t2:s2");
            return null;
         }
      });

      stateSequencer.enter("start", 0, SECONDS);
      Thread.sleep(500);
      assertFalse(future1.isDone());
      assertFalse(future2.isDone());
      assertFalse(t2s2Entered.get());
      stateSequencer.exit("start");

      stateSequencer.advance("stop", 10, SECONDS);

      future1.get();
      future2.get();
   }
}
