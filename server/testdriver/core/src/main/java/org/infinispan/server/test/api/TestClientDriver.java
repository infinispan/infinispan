package org.infinispan.server.test.api;

import org.infinispan.counter.api.CounterManager;

/**
 * Test client driver interface contains the methods we want to expose to be used from test methods
 *
 * @author Katia Aresti
 * @since 11
 */
public interface TestClientDriver {

   /**
    * Get the HotRod instance for hotrod api operations
    *
    * @return {@link HotRodTestClientDriver} instance
    */
   HotRodTestClientDriver hotrod();

   /**
    * Get the REST instance for hotrod api operations
    * @return {@link RestTestClientDriver} instance}
    */
   RestTestClientDriver rest();

   /**
    * Provides the current method name
    *
    * @return String, the method name
    */
   String getMethodName();

   /**
    * Access to the {@link CounterManager} to perform counters operations on tests
    *
    * @return the {@link CounterManager} instance
    */
   CounterManager getCounterManager();
}
