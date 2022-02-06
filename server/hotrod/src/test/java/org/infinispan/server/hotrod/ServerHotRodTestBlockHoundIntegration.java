package org.infinispan.server.hotrod;

import org.infinispan.server.hotrod.counter.impl.TestCounterNotificationManager;
import org.infinispan.server.hotrod.event.EventLogListener;
import org.kohsuke.MetaInfServices;

import com.arjuna.ats.internal.arjuna.coordinator.ReaperThread;
import com.arjuna.ats.internal.arjuna.coordinator.ReaperWorkerThread;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class ServerHotRodTestBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      builder.allowBlockingCallsInside(TestCounterNotificationManager.class.getName(), "accept");

      // Let arjuna block - sometimes its thread will be put in our non blocking thread group
      builder.allowBlockingCallsInside(ReaperThread.class.getName(), "run");
      builder.allowBlockingCallsInside(ReaperWorkerThread.class.getName(), "run");

      builder.allowBlockingCallsInside(EventLogListener.class.getName(), "onCreated");
      builder.allowBlockingCallsInside(EventLogListener.class.getName(), "onModified");
      builder.allowBlockingCallsInside(EventLogListener.class.getName(), "onRemoved");
      builder.allowBlockingCallsInside(EventLogListener.class.getName(), "onCustom");
   }
}
