package org.infinispan.server.hotrod;

import org.infinispan.server.core.MagicByteDetector;

public class HotRodDetector extends MagicByteDetector {
   public static final String NAME = "hotrod-detector";

   public HotRodDetector(HotRodServer hotRodServer) {
      super(hotRodServer, Constants.MAGIC_REQ);
   }

   @Override
   public String getName() {
      return NAME;
   }
}
