package org.infinispan.remoting.transport;

import static org.infinispan.test.TestingUtil.wrapGlobalComponent;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.remoting.responses.Response;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * Controlled {@link org.infinispan.remoting.transport.Transport} that allows to configure commands to block before or
 * after the real invocation or to fail.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class ControlledTransport extends AbstractDelegatingTransport {
   private static final Log log = LogFactory.getLog(ControlledTransport.class);
   private static final Predicate<ReplicableCommand> NEVER = cmd -> false;

   private final ReclosableLatch replicationLatch = new ReclosableLatch(true);
   private final ReclosableLatch blockingLatch = new ReclosableLatch(true);
   private volatile Predicate<ReplicableCommand> blockBeforeFilter = NEVER;
   private volatile Predicate<ReplicableCommand> blockAfterFilter = NEVER;
   private volatile Predicate<ReplicableCommand> failFilter = NEVER;

   private ControlledTransport(Transport realOne) {
      super(realOne);
   }

   public static ControlledTransport replace(Cache<?, ?> cache) {
      return wrapGlobalComponent(cache.getCacheManager(), Transport.class, ControlledTransport::new, true);
   }

   @Override
   public void start() {
      //skip start it again.
   }

   public void failFor(Class... filter) {
      failFor(classListToFilter(filter));
   }

   private void failFor(Predicate<ReplicableCommand> filter) {
      this.failFilter = filter;
      blockingLatch.open();
   }

   public void stopFailing() {
      this.failFilter = NEVER;
      blockingLatch.open();
   }

   public void blockBefore(Class... filter) {
      blockBefore(classListToFilter(filter));
   }

   public void blockBefore(Predicate<ReplicableCommand> filter) {
      this.blockBeforeFilter = filter;
      replicationLatch.close();
      blockingLatch.close();
   }

   public void blockAfter(Class... filter) {
      blockAfter(classListToFilter(filter));
   }

   public void blockAfter(Predicate<ReplicableCommand> filter) {
      this.blockAfterFilter = filter;
      replicationLatch.close();
      blockingLatch.close();
   }

   public void stopBlocking() {
      log.tracef("Stop blocking commands");
      blockBeforeFilter = NEVER;
      blockAfterFilter = NEVER;
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
      if (failFilter.test(rpcCommand)) {
         throw new IllegalStateException("Induced failure!");
      }
   }

   protected void waitBefore(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockBeforeFilter);
   }

   protected void waitAfter(ReplicableCommand rpcCommand) {
      waitForReplicationLatch(rpcCommand, blockAfterFilter);
   }

   protected void waitForReplicationLatch(ReplicableCommand rpcCommand, Predicate<ReplicableCommand> filter) {
      if (!filter.test(rpcCommand)) {
         log.tracef("Not blocking command %s", rpcCommand);
         return;
      }

      try {
         if (!blockingLatch.isOpened()) {
            log.debugf("Replication trigger called, releasing any waiters for command to block.");
            blockingLatch.open();
         }

         log.debugf("Replication trigger called, waiting for latch to open.");
         assertTrue(replicationLatch.await(30, TimeUnit.SECONDS));
         log.trace("Replication latch opened, continuing.");
      } catch (Exception e) {
         throw new RuntimeException("Unexpected exception!", e);
      }
   }

   @Override
   protected void beforeInvokeRemotely(ReplicableCommand command) {
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

   private Predicate<ReplicableCommand> classListToFilter(Class[] filter) {
      return cmd -> {
         Class<?> actualClass = getActualClass(cmd);
         return Stream.of(filter).anyMatch(clazz -> clazz.isAssignableFrom(actualClass));
      };
   }

   private Class getActualClass(ReplicableCommand rpcCommand) {
      Class cmdClass = rpcCommand.getClass();
      if (cmdClass.equals(SingleRpcCommand.class)) {
         cmdClass = ((SingleRpcCommand) rpcCommand).getCommand().getClass();
      }
      return cmdClass;
   }
}
