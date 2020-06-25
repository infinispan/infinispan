package org.infinispan.query.remote.impl;

import org.infinispan.commons.internal.CommonsBlockHoundIntegration;
import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class RemoteQueryServerBlockHoundIntegration implements BlockHoundIntegration {

   @Override
   public void applyTo(BlockHound.Builder builder) {
      // Various methods that need to be removed as they are essentially bugs. Please ensure that a JIRA is created and
      // referenced here for any such method.

      // ProtobufMetadataManagerInterceptor is blocking in quite a few places
      // https://issues.redhat.com/browse/ISPN-11832
      CommonsBlockHoundIntegration.allowMethodsToBlock(builder, ProtobufMetadataManagerInterceptor.class, false);
   }
}
