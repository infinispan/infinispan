package org.infinispan.counter.configuration;

/**
 * Reliability mode available for {@link org.infinispan.counter.api.CounterManager}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public enum Reliability {
   /**
    * If the cluster splits in multiple partition, all of them are allowed to continue read/update the counter.
    * <p>
    * When the cluster merges back, one partition's updates are kept the others partition's updates are lost.
    */
   AVAILABLE,
   /**
    * If the cluster splits in multiple partitions, the majority partition is allowed to read/update the counter if the
    * counter is available on that partition.
    * <p>
    * The minority partition is allowed to read if the counter is available on that partition.
    * <p>
    * When the cluster merges back, the partitions conciliates the counter's value.
    */
   CONSISTENT
}
