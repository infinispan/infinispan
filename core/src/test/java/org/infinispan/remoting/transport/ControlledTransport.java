package org.infinispan.remoting.transport;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.remoting.responses.Response;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.xsite.XSiteReplicateCommand;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Controlled {@link org.infinispan.remoting.transport.Transport} that allows to configure commands to block before or
 * after the real invocation or to fail.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class ControlledTransport extends AbstractDelegatingTransport {

   private final ReclosableLatch replicationLatch = new ReclosableLatch(true);
   private final ReclosableLatch blockingLatch = new ReclosableLatch(true);
   private volatile Set<Class> blockBeforeFilter = Collections.emptySet();
   private volatile Set<Class> blockAfterFilter = Collections.emptySet();
   private volatile Set<Class> failFilter = Collections.emptySet();

   public ControlledTransport(Transport realOne) {
      super(realOne);
   }

   @Override
   public void start() {
      //skip start it again.
   }

   public void failFor(Class... filter) {
      this.failFilter = new HashSet<>(Arrays.asList(filter));
      blockingLatch.open();
   }

   public void stopFailing() {
      this.failFilter = Collections.emptySet();
      blockingLatch.open();
   }

   public void blockBefore(Class... filter) {
      this.blockBeforeFilter = new HashSet<>(Arrays.asList(filter));
      replicationLatch.close();
      blockingLatch.close();
   }

   public void blockAfter(Class... filter) {
      this.blockAfterFilter = new HashSet<>(Arrays.asList(filter));
      replicationLatch.close();
      blockingLatch.close();
   }

   public void stopBlocking() {
      getLog().tracef("Stop blocking commands");
      blockBeforeFilter = Collections.emptySet();
      blockAfterFilter = Collections.emptySet();
      replicationLatch.open();
      blockingLatch.open();
   }

   public void waitForCommandToBlock() throws InterruptedException {
      getLog().tracef("Waiting for at least one command to block");
      blockingLatch.await();
   }

   public boolean waitForCommandToBlock(long time, TimeUnit unit) throws InterruptedException {
      return blockingLatch.await(time, unit);
   }

   public void failIfNeeded(ReplicableCommand rpcCommand) {
      if (failFilter.contains(getActualClass(rpcCommand))) {
         throw new IllegalStateException("Induced failure!");
      }
   }

   protected void waitBefore(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockBeforeFilter);
   }

   protected void waitAfter(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockAfterFilter);
   }

   protected void waitForReplicationLatch(ReplicableCommand rpcCommand, Set<Class> filter) {
      Class cmdClass = getActualClass(rpcCommand);
      if (!filter.contains(cmdClass)) {
         return;
      }

      try {
         if (!blockingLatch.isOpened()) {
            getLog().debugf("Replication trigger called, releasing any waiters for command to block.");
            blockingLatch.open();
         }

         getLog().debugf("Replication trigger called, waiting for latch to open.");
         replicationLatch.await();
         getLog().trace("Replication latch opened, continuing.");
      } catch (Exception e) {
         throw new RuntimeException("Unexpected exception!", e);
      }
   }

   @Override
   protected void beforeInvokeRemotely(ReplicableCommand command, Collection<Address> recipients) {
      failIfNeeded(command);
      waitBefore(command);
   }

   @Override
   protected Map<Address, Response> afterInvokeRemotely(ReplicableCommand command, Map<Address, Response> responseMap) {
      waitAfter(command);
      return responseMap;
   }

   @Override
   protected void beforeBackupRemotely(XSiteReplicateCommand command) {
      failIfNeeded(command);
      waitBefore(command);
   }

   @Override
   protected BackupResponse afterBackupRemotely(ReplicableCommand command, BackupResponse response) {
      waitAfter(command);
      return response;
   }

   private Class getActualClass(ReplicableCommand rpcCommand) {
      Class cmdClass = rpcCommand.getClass();
      if (cmdClass.equals(SingleRpcCommand.class)) {
         cmdClass = ((SingleRpcCommand) rpcCommand).getCommand().getClass();
      }
      return cmdClass;
   }
}
