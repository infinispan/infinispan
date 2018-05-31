package org.infinispan.container.impl;

/**
 * This interface is <strong>not</strong> a public API.
 *
 * <p>A value implementing this interface is merged with the actual value when storing
 * into {@link org.infinispan.container.DataContainer}.
 * As the merge operation is synchronized externally, the implementation of this interface does not need to be
 * thread-safe. Note that the value in context (last written value) needs to implement {@link MergeOnStore},
 * if the cache already containing such instance is overwritten by a non-implementor, the merge does not happen.
 *
 * <p>The intended use is when executing a command with {@link org.infinispan.context.Flag#SKIP_LOCKING}; in that case
 * we may have two different instances in context at one moment and the writes may be applied in any order, even
 * in a different order on different owners. Therefore it's strongly recommended that all operations on such entry
 * are commutative (order-independent).
 *
 * <p>As the atomicity of load & store into persistence layer cannot be guaranteed, it is recommended to use such values
 * only with {@link org.infinispan.context.Flag#SKIP_CACHE_LOAD} and
 * {@link org.infinispan.context.Flag#SKIP_CACHE_STORE} or implementing a custom externalizer that will deal with this.
 */
public interface MergeOnStore {
   /**
    * @param other Actual value stored in the cache, or <code>null</code> if the entry does not exist.
    * @return Value that will be stored in the cache, or null if it should be removed.
    */
   Object merge(Object other);
}
