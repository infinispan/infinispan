package org.infinispan.commons.test;

import reactor.blockhound.BlockHound;

class BlockHoundHelper {
   private BlockHoundHelper() { }

   /**
    * Installs BlockHound and all service loaded integrations to ensure that blocking doesn't occur where it shouldn't
    */
   static void installBlockHound() {
      // Automatically registers all services that implement BlockHoundIntegration interface
      BlockHound.install();
   }
}
