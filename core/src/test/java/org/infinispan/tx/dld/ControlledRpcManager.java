package org.infinispan.tx.dld;

import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.AbstractControlledRpcManager;
import org.infinispan.util.concurrent.ReclosableLatch;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class ControlledRpcManager extends AbstractControlledRpcManager {

   private final ReclosableLatch replicationLatch = new ReclosableLatch(true);
   private final ReclosableLatch blockingLatch = new ReclosableLatch(true);
   private volatile Set<Class> blockBeforeFilter = Collections.emptySet();
   private volatile Set<Class> blockAfterFilter = Collections.emptySet();
   private volatile Set<Class> failFilter = Collections.emptySet();
   private final ArrayList<BiConsumer<ReplicableCommand, Object>> responseChecks = new ArrayList<>();

   public ControlledRpcManager(RpcManager realOne) {
      super(realOne);
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
      log.tracef("Stop blocking commands");
      blockBeforeFilter = Collections.emptySet();
      blockAfterFilter = Collections.emptySet();
      replicationLatch.open();
      blockingLatch.open();
   }

   public void waitForCommandToBlock() throws InterruptedException {
      log.tracef("Waiting for at least one command to block");
      assertTrue(blockingLatch.await(30, TimeUnit.SECONDS));
   }

   public boolean waitForCommandToBlock(long time, TimeUnit unit) throws InterruptedException {
      return blockingLatch.await(time, unit);
   }

   public void failIfNeeded(ReplicableCommand rpcCommand) {
      if (failFilter.contains(getActualClass(rpcCommand))) {
         throw new IllegalStateException("Induced failure!");
      }
   }

   public <T> void checkResponses(Consumer<T> checker) {
      responseChecks.add((c, responseObject) -> checker.accept((T) responseObject));
   }

   public <T> void checkResponses(BiConsumer<ReplicableCommand, T> checker) {
      responseChecks.add((BiConsumer<ReplicableCommand, Object>) checker);
   }

   protected void waitBefore(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockBeforeFilter);
   }

   protected boolean waitAfter(ReplicableCommand rpcCommand) {
      return waitForReplicationLatch(rpcCommand, blockAfterFilter);
   }

   protected boolean waitForReplicationLatch(ReplicableCommand rpcCommand, Set<Class> filter) {
      Class cmdClass = getActualClass(rpcCommand);
      if (!filter.contains(cmdClass)) {
         return false;
      }

      try {
         if (!blockingLatch.isOpened()) {
            log.debugf("Replication trigger called, releasing any waiters for command to block.");
            blockingLatch.open();
         }

         log.debugf("Replication trigger called, waiting for latch to open.");
         assertTrue(replicationLatch.await(30, TimeUnit.SECONDS));
         log.trace("Replication latch opened, continuing.");
         return true;
      } catch (Exception e) {
         throw new RuntimeException("Unexpected exception!", e);
      }
   }

   @Override
   protected Object beforeInvokeRemotely(Collection<Address> targets, ReplicableCommand command) {
      failIfNeeded(command);
      waitBefore(command);
      return null;
   }

   @Override
   protected <T> T afterInvokeRemotely(Collection<Address> targets, ReplicableCommand command, T responseObject, Object argument) {
      if (waitAfter(command)) {
         responseChecks.forEach(check -> check.accept(command, responseObject));
      }
      return responseObject;
   }

   private Class getActualClass(ReplicableCommand rpcCommand) {
      Class cmdClass = rpcCommand.getClass();
      if (cmdClass.equals(SingleRpcCommand.class)) {
         cmdClass = ((SingleRpcCommand) rpcCommand).getCommand().getClass();
      }
      return cmdClass;
   }
}
