package org.infinispan.hotrod.configuration;

/**
 * Enumeration for whenExhaustedAction. Order is important, as the underlying commons-pool uses a byte to represent values
 * ExhaustedAction.
 *
 * @since 14.0
 */
public enum ExhaustedAction {
   EXCEPTION, // GenericKeyedObjectPool.WHEN_EXHAUSTED_FAIL
   WAIT, // GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK
   CREATE_NEW // GenericKeyedObjectPool.WHEN_EXHAUSTED_GROW
}
