package org.infinispan.rest;

import org.infinispan.query.mapper.mapping.SearchMappingBuilder;
import org.kohsuke.MetaInfServices;

import com.arjuna.ats.internal.arjuna.coordinator.ReaperThread;
import com.arjuna.ats.internal.arjuna.coordinator.ReaperWorkerThread;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class ServerRestTestBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      // Let arjuna block - sometimes its thread will be put in our non blocking thread group
      builder.allowBlockingCallsInside(ReaperThread.class.getName(), "run");
      builder.allowBlockingCallsInside(ReaperWorkerThread.class.getName(), "run");
      // java.util.ServiceLoader is used during build().
      builder.allowBlockingCallsInside(SearchMappingBuilder.class.getName(), "build");

      // `DistributedStream` is blocking.
      builder.markAsBlocking("io.reactivex.rxjava3.internal.operators.flowable.BlockingFlowableIterable$BlockingFlowableIterator", "next", "()Ljava/lang/Object;");
      builder.markAsBlocking("io.reactivex.rxjava3.internal.operators.flowable.BlockingFlowableIterable$BlockingFlowableIterator", "hasNext", "()Z");
   }
}
