package org.infinispan.server.hotrod;

import org.infinispan.server.hotrod.counter.impl.TestCounterNotificationManager;
import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class ServerHotRodTestBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      builder.allowBlockingCallsInside(TestCounterNotificationManager.class.getName(), "accept");
   }
}
