package org.infinispan.persistence.spi;

/**
 * Implemented by stores that can skip writes based on certain flags present in the invocation.
 * @since 9.0
 */
public interface FlagAffectedStore<K, V> extends ExternalStore<K, V> {

   boolean shouldWrite(long commandFlags);

}
