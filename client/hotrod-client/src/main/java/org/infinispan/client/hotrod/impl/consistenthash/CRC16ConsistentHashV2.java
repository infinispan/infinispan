package org.infinispan.client.hotrod.impl.consistenthash;

import java.util.Random;

import org.infinispan.commons.hash.CRC16;

public class CRC16ConsistentHashV2 extends ConsistentHashV2 {

   public CRC16ConsistentHashV2(Random rnd) {
      super(rnd, CRC16.getInstance());
   }

   public CRC16ConsistentHashV2() {
      this(new Random());
   }
}
