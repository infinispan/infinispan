package org.infinispan.counter.api;

import java.util.Collection;

/**
 * The {@link CounterManager} creates, defines and returns counters.
 * <p>
 * It is thread-safe in the way that multiples threads can retrieve/create counters concurrently. If it is the first
 * time a counter is created, other concurrent threads may block until it is properly initialized.
 * <p>
 * A counter can be defined using {@link CounterManager#defineCounter(String, CounterConfiguration)} and {@link
 * CounterManager#isDefined(String)} returns if the counter is defined or not.
 * <p>
 * The counter can be retrieved/created using the {@link CounterManager#getStrongCounter(String)} or {@link
 * CounterManager#getWeakCounter(String)} to return an (un)bounded strong counter or weak counter. The operation will
 * fail if the counter is defined with a different type. For example, define a strong counter {@code "test"} and try to
 * retrieve using the {@code getWeakCounter("test"}.
 *
 * @author Pedro Ruivo
 * @see CounterConfiguration
 * @since 9.0
 */
public interface CounterManager {

   /**
    * Returns the {@link StrongCounter} with that specific name.
    * <p>
    * If the {@link StrongCounter} does not exists, it is created based on the {@link CounterConfiguration}.
    * <p>
    * Note that the counter must be defined prior to this method invocation using {@link
    * CounterManager#defineCounter(String, CounterConfiguration)} or via global configuration. This method only supports
    * {@link CounterType#BOUNDED_STRONG} and {@link CounterType#UNBOUNDED_STRONG} counters.
    *
    * @param name the counter name.
    * @return the {@link StrongCounter} instance.
    * @throws org.infinispan.counter.exception.CounterException              if unable to retrieve the counter.
    * @throws org.infinispan.counter.exception.CounterConfigurationException if the counter configuration is not valid
    *                                                                        or it does not exists.
    */
   StrongCounter getStrongCounter(String name);

   /**
    * Returns the {@link WeakCounter} with that specific name.
    * <p>
    * If the {@link WeakCounter} does not exists, it is created based on the {@link CounterConfiguration}.
    * <p>
    * Note that the counter must be defined prior to this method invocation using {@link
    * CounterManager#defineCounter(String, CounterConfiguration)} or via global configuration. This method only supports
    * {@link CounterType#WEAK} counters.
    *
    * @param name the counter name.
    * @return the {@link WeakCounter} instance.
    * @throws org.infinispan.counter.exception.CounterException              if unable to retrieve the counter.
    * @throws org.infinispan.counter.exception.CounterConfigurationException if the counter configuration is not valid
    *                                                                        or it does not exists.
    */
   WeakCounter getWeakCounter(String name);

   /**
    * Defines a counter with the specific {@code name} and {@link CounterConfiguration}.
    * <p>
    * It does not overwrite existing configurations.
    *
    * @param name          the counter name.
    * @param configuration the counter configuration
    * @return {@code true} if successfully defined or {@code false} if the counter exists or fails to defined.
    */
   boolean defineCounter(String name, CounterConfiguration configuration);

   /**
    * It removes the counter and its configuration from the cluster.
    *
    * @param name The counter's name to remove
    */
   void undefineCounter(String name);

   /**
    * @param name the counter name.
    * @return {@code true} if the counter is defined or {@code false} if the counter is not defined or fails to check.
    */
   boolean isDefined(String name);

   /**
    * @param counterName the counter name.
    * @return the counter's {@link CounterConfiguration} or {@code null} if the counter is not defined.
    */
   CounterConfiguration getConfiguration(String counterName);

   /**
    * It removes the counter from the cluster.
    * <p>
    * All instances returned by {@link #getWeakCounter(String)} or {@link #getStrongCounter(String)} are destroyed and
    * they shouldn't be used anymore. Also, the registered {@link CounterListener}s are removed and they aren't invoked
    * anymore.
    *
    * @param counterName The counter's name to remove.
    */
   void remove(String counterName);

   /**
    * Returns a {@link Collection} of defined counter names.
    *
    * @return a {@link Collection} of defined counter names.
    */
   Collection<String> getCounterNames();

}
