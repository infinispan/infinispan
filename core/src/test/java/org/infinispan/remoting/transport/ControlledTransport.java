package org.infinispan.remoting.transport;

import static org.infinispan.test.TestingUtil.wrapGlobalComponent;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
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
      return replace(cache.getCacheManager());
   }

   public static ControlledTransport replace(EmbeddedCacheManager manager) {
      log.tracef("Replacing transport on %s", manager.getAddress());
      return wrapGlobalComponent(manager, Transport.class, ControlledTransport::new, true);
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

   public <T extends ReplicableCommand> void blockBefore(Class<T> commandClass, Predicate<T> filter) {
      blockBefore(c -> commandClass.isInstance(c) && filter.test(commandClass.cast(c)));
   }

   public void blockBefore(Predicate<ReplicableCommand> filter) {
      this.blockBeforeFilter = filter;
      replicationLatch.close();
      blockingLatch.close();
   }

   public void blockAfter(Class... filter) {
      blockAfter(classListToFilter(filter));
   }

   public <T extends ReplicableCommand> void blockAfter(Class<T> commandClass, Predicate<T> filter) {
      blockAfter(c -> commandClass.isInstance(c) && filter.test(commandClass.cast(c)));
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
      log.tracef("Waiting for at least one command to block on %s", actual.getAddress());
      assertTrue(blockingLatch.await(30, TimeUnit.SECONDS));
   }

   public boolean waitForCommandToBlock(long time, TimeUnit unit) throws InterruptedException {
      log.tracef("Waiting for at least one command to block");
      return blockingLatch.await(time, unit);
   }

   public void failIfNeeded(ReplicableCommand rpcCommand) {
      if (failFilter.test(rpcCommand)) {
         log.tracef("Failing remote invocation of " + rpcCommand);
         throw new IllegalStateException("Induced failure!");
      }
   }

   protected void waitBefore(ReplicableCommand rpcCommand) {
      waitForReplicationLatch("before", rpcCommand, blockBeforeFilter);
   }

   protected void waitAfter(ReplicableCommand rpcCommand) {
      waitForReplicationLatch("after", rpcCommand, blockAfterFilter);
   }

   protected void waitForReplicationLatch(String when, ReplicableCommand rpcCommand, Predicate<ReplicableCommand> filter) {
      if (!filter.test(rpcCommand)) {
         log.tracef("Not blocking %s command %s", when, rpcCommand);
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
   public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                               ResponseCollector<T> collector, DeliverOrder deliverOrder, long timeout,
                                               TimeUnit unit) {
      failIfNeeded(command);
      waitBefore(command);
      return super.invokeCommand(target, command, collector, deliverOrder, timeout, unit)
                  .whenComplete((ignored, throwable) -> waitAfter(command));
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                               ResponseCollector<T> collector, DeliverOrder deliverOrder, long timeout,
                                               TimeUnit unit) {
      failIfNeeded(command);
      waitBefore(command);
      return super.invokeCommand(targets, command, collector, deliverOrder, timeout, unit)
                  .whenComplete((ignored, throwable) -> waitAfter(command));
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                    DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      failIfNeeded(command);
      waitBefore(command);
      return super.invokeCommandOnAll(command, collector, deliverOrder, timeout, unit)
                  .whenComplete((ignored, throwable) -> waitAfter(command));
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(Collection<Address> requiredTargets, ReplicableCommand command,
                                                    ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                                    long timeout, TimeUnit unit) {
      failIfNeeded(command);
      waitBefore(command);
      return super.invokeCommandOnAll(requiredTargets, command, collector, deliverOrder, timeout, unit)
                  .whenComplete((ignored, throwable) -> waitAfter(command));
   }

   @Override
   public <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                                Function<Address, ReplicableCommand> commandGenerator,
                                                ResponseCollector<T> collector, DeliverOrder deliverOrder, long timeout,
                                                TimeUnit timeUnit) {
      AtomicReference<Object> result = new AtomicReference<>(null);
      ResponseCollector<T> partCollector = new ResponseCollector<T>() {
         @Override
         public T addResponse(Address sender, Response response) {
            synchronized (this) {
               if (result.get() != null)
                  return null;

               result.set(collector.addResponse(sender, response));
               return null;
            }
         }

         @Override
         public T finish() {
            // Do nothing when individual commands finish
            return null;
         }
      };
      AggregateCompletionStage<Void> allStage = CompletionStages.aggregateCompletionStage();
      for (Address target : targets) {
         allStage.dependsOn(invokeCommand(target, commandGenerator.apply(target), partCollector, deliverOrder,
                                   timeout, timeUnit));
      }
      return allStage.freeze().thenApply(v -> {
         synchronized (partCollector) {
            if (result.get() != null) {
               return (T) result.get();
            } else {
               return collector.finish();
            }
         }
      });
   }

   @Override
   public <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                                Function<Address, ReplicableCommand> commandGenerator,
                                                ResponseCollector<T> responseCollector, long timeout,
                                                DeliverOrder deliverOrder) {
      return invokeCommands(targets, commandGenerator, responseCollector, deliverOrder, timeout, TimeUnit.MILLISECONDS);
   }

   @Override
   public XSiteResponse backupRemotely(XSiteBackup backup, XSiteReplicateCommand rpcCommand) {
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      XSiteResponse response = super.backupRemotely(backup, rpcCommand);
      // Hack: assumes that the wait here will block dependents added later
      response.whenComplete((ignored, throwable) -> waitAfter(rpcCommand));
      return response;
   }

   @Override
   public BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteReplicateCommand rpcCommand)
         throws Exception {
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      BackupResponse response = super.backupRemotely(backups, rpcCommand);
      // Hack: assumes that the wait here will block dependents added later
      response.notifyFinish(ignored -> waitAfter(rpcCommand));
      response.notifyAsyncAck((sendTimestampNanos, siteName, throwable) -> waitAfter(rpcCommand));
      return response;
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      super.sendTo(destination, rpcCommand, deliverOrder);
      waitAfter(rpcCommand);
   }

   @Override
   public void sendToMany(Collection<Address> destinations, ReplicableCommand rpcCommand, DeliverOrder deliverOrder)
         throws Exception {
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      super.sendToMany(destinations, rpcCommand, deliverOrder);
      waitAfter(rpcCommand);
   }

   @Override
   public void sendToAll(ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      super.sendToAll(rpcCommand, deliverOrder);
      waitAfter(rpcCommand);
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand rpcCommand, ResponseMode mode,
                                                                        long timeout, ResponseFilter responseFilter,
                                                                        DeliverOrder deliverOrder, boolean anycast)
         throws Exception {
      failIfNeeded(rpcCommand);
      waitBefore(rpcCommand);
      return super.invokeRemotelyAsync(recipients, rpcCommand, mode, timeout, responseFilter, deliverOrder, anycast)
                  .whenComplete((ignored, throwable) -> waitAfter(rpcCommand));
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode,
                                                long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder,
                                                boolean anycast) throws Exception {
      throw new UnsupportedOperationException();
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode,
                                                long timeout, boolean usePriorityQueue, ResponseFilter responseFilter,
                                                boolean totalOrder, boolean anycast) throws Exception {
      throw new UnsupportedOperationException();
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand,
                                                ResponseMode mode, long timeout, ResponseFilter responseFilter,
                                                DeliverOrder deliverOrder, boolean anycast) throws Exception {
      throw new UnsupportedOperationException();
   }

   private Predicate<ReplicableCommand> classListToFilter(Class<?>[] filter) {
      return cmd -> {
         Class<?> actualClass = getActualClass(cmd);
         return Stream.of(filter).anyMatch(clazz -> clazz.isAssignableFrom(actualClass));
      };
   }

   private Class<? extends ReplicableCommand> getActualClass(ReplicableCommand rpcCommand) {
      Class<? extends ReplicableCommand> cmdClass = rpcCommand.getClass();
      if (cmdClass.equals(SingleRpcCommand.class)) {
         cmdClass = ((SingleRpcCommand) rpcCommand).getCommand().getClass();
      }
      return cmdClass;
   }
}
