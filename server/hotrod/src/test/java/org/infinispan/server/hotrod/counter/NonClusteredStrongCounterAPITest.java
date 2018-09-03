package org.infinispan.server.hotrod.counter;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.server.hotrod.HotRodVersion;
import org.infinispan.server.hotrod.counter.impl.StrongCounterImplTestStrategy;
import org.infinispan.server.hotrod.counter.impl.TestCounterManager;
import org.testng.annotations.Test;

/**
 * A {@link org.infinispan.counter.api.StrongCounter} api test.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Test(groups = "functional", testName = "server.hotrod.counter.NonClusteredStrongCounterAPITest")
public class NonClusteredStrongCounterAPITest extends HotRodSingleNodeTest implements StrongCounterTestStrategy {

   private final StrongCounterTestStrategy strategy;

   public NonClusteredStrongCounterAPITest() {
      strategy = new StrongCounterImplTestStrategy(this::testCounterManager, this::allTestCounterManager);
   }

   @Override
   public void testCompareAndSet(Method method) {
      strategy.testCompareAndSet(method);
   }

   @Override
   public void testCompareAndSwap(Method method) {
      strategy.testCompareAndSwap(method);
   }

   @Override
   public void testBoundaries(Method method) {
      strategy.testBoundaries(method);
   }

   @Override
   public void testAdd(Method method) {
      strategy.testAdd(method);
   }

   @Override
   public void testReset(Method method) {
      strategy.testReset(method);
   }

   @Override
   public void testNameAndConfigurationTest(Method method) {
      strategy.testNameAndConfigurationTest(method);
   }

   @Override
   public void testRemove(Method method) {
      strategy.testRemove(method);
   }

   @Override
   public void testListenerAddAndRemove(Method method) throws InterruptedException {
      strategy.testListenerAddAndRemove(method);
   }

   @Override
   public void testListenerWithBounds(Method method) throws InterruptedException {
      strategy.testListenerWithBounds(method);
   }

   @Override
   protected byte protocolVersion() {
      return HotRodVersion.HOTROD_27.getVersion();
   }

   private CounterManager testCounterManager() {
      return new TestCounterManager(client());
   }

   private Collection<CounterManager> allTestCounterManager() {
      return Collections.singleton(testCounterManager());
   }

}
