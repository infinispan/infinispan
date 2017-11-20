package org.infinispan.server.hotrod.counter;

import java.lang.reflect.Method;

import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * A base test interface to test common methods from {@link StrongCounter} and {@link WeakCounter} API.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public interface BaseCounterTestStrategy {

   void testAdd(Method method);

   void testReset(Method method);

   void testNameAndConfigurationTest(Method method);

   void testRemove(Method method);

   void testListenerAddAndRemove(Method method) throws InterruptedException;
}
