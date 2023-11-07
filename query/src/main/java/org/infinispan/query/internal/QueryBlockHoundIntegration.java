package org.infinispan.query.internal;

import org.apache.lucene.util.NamedSPILoader;
import org.hibernate.search.engine.backend.work.execution.OperationSubmitter;
import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class QueryBlockHoundIntegration implements BlockHoundIntegration {

   @Override
   public void applyTo(BlockHound.Builder builder) {
      // Loading a service may require opening a file from classpath
      builder.allowBlockingCallsInside(NamedSPILoader.class.getName(), "reload");

      // With Infinispan the implementation is OffloadingExecutorOperationSubmitter.
      // Here the submitToQueue method invokes BlockingQueue#offer, the single argument one,
      // that uses the lock of the blocking queue just to check safely the size of the data structure,
      // releasing immediately the lock after this super-short check.
      builder.allowBlockingCallsInside(OperationSubmitter.class.getName() + "$OffloadingExecutorOperationSubmitter",
            "submitToQueue");
   }
}
