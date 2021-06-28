package org.infinispan.rest;

import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class ServerRestBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      // ChunkedFile read is blocking - This can be fixed in
      // https://issues.redhat.com/browse/ISPN-11834
      builder.allowBlockingCallsInside(ResponseWriter.CHUNKED_FILE.getClass().getName(), "writeResponse");
      // ChunkedInputStream read is blocking, see ISPN-13131
      builder.allowBlockingCallsInside(ResponseWriter.CHUNKED_STREAM.getClass().getName(), "writeResponse");
   }
}
