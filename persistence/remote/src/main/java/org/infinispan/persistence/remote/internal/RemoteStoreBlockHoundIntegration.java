package org.infinispan.persistence.remote.internal;

import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.commons.internal.CommonsBlockHoundIntegration;
import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class RemoteStoreBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      // TODO: this needs to be moved to the client hotrod module when it adds BlockHound
      // https://issues.redhat.com/browse/ISPN-12180
      builder.allowBlockingCallsInside("org.infinispan.client.hotrod.impl.transport.netty.ChannelInitializer", "initSsl");
      CommonsBlockHoundIntegration.allowPublicMethodsToBlock(builder, ChannelFactory.class);
   }
}
