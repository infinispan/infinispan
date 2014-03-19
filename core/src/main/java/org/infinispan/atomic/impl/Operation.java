package org.infinispan.atomic.impl;

import java.util.Map;

/**
 * An atomic operation.
 * <p/>
 *
 * @author (various)
 * @param <K>
 * @param <V>
 * @since 4.0
 */
public abstract class Operation<K, V> {
   
   public abstract K keyAffected();
   
   public abstract void replay(Map<K, V> delegate);

   public abstract void rollback(Map<K, V> delegate);
   
}
