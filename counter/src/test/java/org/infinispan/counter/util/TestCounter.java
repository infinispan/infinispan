package org.infinispan.counter.util;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;

/**
 * A  common interface to use in tests for {@link org.infinispan.counter.api.StrongCounter} and {@link
 * org.infinispan.counter.api.WeakCounter}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface TestCounter {

   <T extends CounterListener> Handle<T> addListener(T listener);

   void increment();

   void add(long delta);

   void decrement();

   long getValue();

   void reset();

   String getName();

   CounterConfiguration getConfiguration();

   boolean isSame(TestCounter other);

   void remove();
}
