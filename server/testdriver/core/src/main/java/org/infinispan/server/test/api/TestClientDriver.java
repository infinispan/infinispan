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
    * Get the REST instance
    * @return {@link RestTestClientDriver} instance}
    */
   RestTestClientDriver rest();

   /**
    * Get the RESP instance.
    * @return {@link RespTestClientDriver} instance.
    */
   RespTestClientDriver resp();

   /**
    * Get the Memcached instance
    * @return {@link MemcachedTestClientDriver} instance}
    */
   MemcachedTestClientDriver memcached();

   JmxTestClient jmx();

   /**
    * Returns a unique identifier for the current test method
    *
    * @return String, the identifier
    */
   String getMethodName();

   /**
    * Returns a unique identifier for the current test method
    *
    * @param qualifier an additional qualifier
    * @return String, the identifier
    */
   String getMethodName(String qualifier);

   /**
    * Access to the {@link CounterManager} to perform counters operations on tests
    *
    * @return the {@link CounterManager} instance
    */
   CounterManager getCounterManager();
}
