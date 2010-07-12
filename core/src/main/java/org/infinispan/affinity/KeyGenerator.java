package org.infinispan.affinity;

import java.io.IOException;

/**
 * Used for generating keys; used by {@link org.infinispan.affinity.KeyAffinityService} to generate the affinity keys.
 * It offers the possibility to generate keys in a particular format.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface KeyGenerator<K> {
   public K getKey();
}
