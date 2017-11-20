package org.infinispan.server.hotrod.counter;

import java.lang.reflect.Method;

import org.infinispan.counter.api.CounterManager;

/**
 * A interface for testing the {@link CounterManager} API.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public interface CounterManagerTestStrategy {

   void testWeakCounter(Method method);

   void testUnboundedStrongCounter(Method method);

   void testUpperBoundedStrongCounter(Method method);

   void testLowerBoundedStrongCounter(Method method);

   void testBoundedStrongCounter(Method method);

   void testUndefinedCounter();

   void testRemove(Method method);

   void testGetCounterNames(Method method);
}
