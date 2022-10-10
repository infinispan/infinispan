package org.infinispan.server.memcached.test;

import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

/**
 * @since 14.0
 */
@MetaInfServices
public class MemcachedBlockHoundIntegration implements BlockHoundIntegration {

   @Override
   public void applyTo(BlockHound.Builder builder) {
      builder.allowBlockingCallsInside(MemcachedTestingUtil.class.getName() + "$2", "getDecoder");
   }
}
