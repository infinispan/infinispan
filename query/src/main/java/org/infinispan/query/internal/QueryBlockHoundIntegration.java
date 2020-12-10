package org.infinispan.query.internal;

import org.apache.lucene.util.NamedSPILoader;
import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class QueryBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      // Loading a service may require opening a file from classpath
      builder.allowBlockingCallsInside(NamedSPILoader.class.getName(), "reload");

      // This should be made non blocking
      // https://issues.redhat.com/browse/ISPN-12569
      builder.allowBlockingCallsInside("org.infinispan.search.mapper.scope.impl.SearchWorkspaceImpl", "purge");
   }
}
