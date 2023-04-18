package org.infinispan.query.remote.impl;

import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class RemoteQueryServerBlockHoundIntegration implements BlockHoundIntegration {

   @Override
   public void applyTo(BlockHound.Builder builder) {
      // the registration of a new ProtoSchema is invoked infrequently enough
      builder.allowBlockingCallsInside(ProtobufMetadataManagerInterceptor.class.getName(), "registerFileDescriptorSource");
   }
}
