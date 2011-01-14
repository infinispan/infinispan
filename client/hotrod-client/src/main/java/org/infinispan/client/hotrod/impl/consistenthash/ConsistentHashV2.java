package org.infinispan.client.hotrod.impl.consistenthash;

import org.infinispan.util.hash.MurmurHash3;

/**
 * Version 2 of the ConsistentHash function.  Uses MurmurHash3.
 *
 * @author manik
 * @see org.infinispan.util.hash.MurmurHash3
 * @since 5.0
 */
public class ConsistentHashV2 extends ConsistentHashV1 {
   public ConsistentHashV2() {
      mmh = new MurmurHash3();
   }
}
