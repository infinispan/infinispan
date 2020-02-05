package org.infinispan.remoting.inboundhandler;

import static org.infinispan.factories.KnownComponentNames.REMOTE_COMMAND_EXECUTOR;
import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.HeartBeatCommand;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.BackupReceiverRepository;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * {@link org.infinispan.remoting.inboundhandler.InboundInvocationHandler} implementation that handles all the {@link
 * org.infinispan.commands.ReplicableCommand}.
 * <p/>
 * This component handles the {@link org.infinispan.commands.ReplicableCommand} from local and remote site. The remote
 * site {@link org.infinispan.commands.ReplicableCommand} are sent to the {@link org.infinispan.xsite.BackupReceiver} to
 * be handled.
 * <p/>
 * Also, the non-{@link org.infinispan.commands.remote.CacheRpcCommand} are processed directly and the {@link
 * org.infinispan.commands.remote.CacheRpcCommand} are processed in the cache's {@link
 * org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
@Scope(Scopes.GLOBAL)
public class GlobalInboundInvocationHandler implements InboundInvocationHandler {

   private static final Log log = LogFactory.getLog(GlobalInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject @ComponentName(REMOTE_COMMAND_EXECUTOR)
   ExecutorService remoteCommandsExecutor;
   @Inject BackupReceiverRepository backupReceiverRepository;
   @Inject GlobalComponentRegistry globalComponentRegistry;

   private static Response shuttingDownResponse() {
      return CacheNotFoundResponse.INSTANCE;
   }

   private static ExceptionResponse exceptionHandlingCommand(Throwable throwable) {
      if (throwable instanceof Exception) {
         return new ExceptionResponse(((Exception) throwable));
      } else {
         return new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
      }
   }

   @Override
   public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
      command.setOrigin(origin);
      try {
         if (command.getCommandId() == HeartBeatCommand.COMMAND_ID) {
            reply.reply(null);
         } else if (command instanceof CacheRpcCommand) {
            handleCacheRpcCommand(origin, (CacheRpcCommand) command, reply, order);
         } else {
            handleReplicableCommand(origin, command, reply, order);
         }
      } catch (Throwable t) {
         CLUSTER.exceptionHandlingCommand(command, t);
         reply.reply(exceptionHandlingCommand(t));
      }
   }

   @Override
   public void handleFromRemoteSite(String origin, XSiteReplicateCommand command, Reply reply, DeliverOrder order) {
      if (trace) {
         log.tracef("Handling command %s from remote site %s", command, origin);
      }
      // TODO BackupReceiver can be merge in PerCacheInboundInvocationHandler
      // It doesn't make sense to have a separate class for it
      BackupReceiver receiver = backupReceiverRepository.getBackupReceiver(origin, command.getCacheName().toString());
      ComponentRegistry cr = receiver.getCache().getAdvancedCache().getComponentRegistry();
      PerCacheInboundInvocationHandler handler = cr.getPerCacheInboundInvocationHandler();
      if (handler != null) { //not a local cache.
         handler.registerXSiteCommandReceiver(reply != Reply.NO_OP);
      }
      command.performInLocalSite(receiver, order.preserveOrder()).whenComplete(new ResponseConsumer(command, reply));
   }

   private void handleCacheRpcCommand(Address origin, CacheRpcCommand command, Reply reply, DeliverOrder mode) {
      if (trace) {
         log.tracef("Attempting to execute CacheRpcCommand: %s [sender=%s]", command, origin);
      }
      ByteString cacheName = command.getCacheName();
      ComponentRegistry cr = globalComponentRegistry.getNamedComponentRegistry(cacheName);

      if (cr == null) {
         if (trace) {
            log.tracef("Silently ignoring that %s cache is not defined", cacheName);
         }
         reply.reply(CacheNotFoundResponse.INSTANCE);
         return;
      }
      CommandsFactory commandsFactory = cr.getCommandsFactory();
      // initialize this command with components specific to the intended cache instance
      commandsFactory.initializeReplicableCommand(command, true);
      PerCacheInboundInvocationHandler handler = cr.getPerCacheInboundInvocationHandler();
      handler.handle(command, reply, mode);
   }

   private void handleReplicableCommand(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
      if (trace) {
         log.tracef("Attempting to execute non-CacheRpcCommand: %s [sender=%s]", command, origin);
      }
      Runnable runnable = new ReplicableCommandRunner(command, reply, globalComponentRegistry, order.preserveOrder());
      if (order.preserveOrder() || !command.canBlock()) {
         //we must/can run in this thread
         runnable.run();
      } else {
         remoteCommandsExecutor.execute(runnable);
      }
   }

   private static class ReplicableCommandRunner extends ResponseConsumer implements Runnable {

      private final GlobalComponentRegistry globalComponentRegistry;
      private final boolean preserveOrder;

      private ReplicableCommandRunner(ReplicableCommand command, Reply reply,
            GlobalComponentRegistry globalComponentRegistry, boolean preserveOrder) {
         super(command, reply);
         this.globalComponentRegistry = globalComponentRegistry;
         this.preserveOrder = preserveOrder;
      }

      @Override
      public void run() {
         try {
            CompletionStage<?> stage;
            if (command instanceof GlobalRpcCommand) {
               stage = ((GlobalRpcCommand) command).invokeAsync(globalComponentRegistry).whenComplete(this);
            } else {
               globalComponentRegistry.wireDependencies(command);
               stage = command.invokeAsync().whenComplete(this);
            }
            if (preserveOrder) {
               CompletionStages.join(stage);
            }
         } catch (Throwable throwable) {
            accept(null, throwable);
         }
      }
   }

   private static class ResponseConsumer implements BiConsumer<Object, Throwable> {

      final ReplicableCommand command;
      private final Reply reply;

      private ResponseConsumer(ReplicableCommand command, Reply reply) {
         this.command = command;
         this.reply = reply;
      }

      @Override
      public void accept(Object retVal, Throwable throwable) {
         reply.reply(convertToResponse(retVal, throwable));
      }

      private Response convertToResponse(Object retVal, Throwable throwable) {
         if (throwable != null) {
            throwable = CompletableFutures.extractException(throwable);
            if (throwable instanceof InterruptedException || throwable instanceof IllegalLifecycleStateException) {
               CLUSTER.debugf("Shutdown while handling command %s", command);
               return shuttingDownResponse();
            } else {
               CLUSTER.exceptionHandlingCommand(command, throwable);
               return exceptionHandlingCommand(throwable);
            }
         } else {
            if (retVal == null || retVal instanceof Response) {
               return (Response) retVal;
            } else {
               return SuccessfulResponse.create(retVal);
            }
         }
      }
   }

}
