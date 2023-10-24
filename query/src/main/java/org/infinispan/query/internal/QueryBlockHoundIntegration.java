package org.infinispan.query.internal;

import org.apache.lucene.util.NamedSPILoader;
import org.infinispan.query.impl.massindex.DistributedIndexerLock;
import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class QueryBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      // Loading a service may require opening a file from classpath
      builder.allowBlockingCallsInside(NamedSPILoader.class.getName(), "reload");
      builder.allowBlockingCallsInside(DistributedIndexerLock.class.getName(), "lock");
   }
}
