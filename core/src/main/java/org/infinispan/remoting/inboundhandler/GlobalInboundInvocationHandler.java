package org.infinispan.remoting.inboundhandler;

import static org.infinispan.factories.KnownComponentNames.REMOTE_COMMAND_EXECUTOR;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
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
import org.infinispan.util.ByteString;
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
   private ExecutorService remoteCommandsExecutor;
   @Inject private BackupReceiverRepository backupReceiverRepository;
   @Inject private GlobalComponentRegistry globalComponentRegistry;

   private static Response shuttingDownResponse() {
      return CacheNotFoundResponse.INSTANCE;
   }

   private static ExceptionResponse exceptionHandlingCommand(Throwable throwable) {
      return new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
   }

   @Override
   public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
      command.setOrigin(origin);
      try {
         if (command instanceof CacheRpcCommand) {
            handleCacheRpcCommand(origin, (CacheRpcCommand) command, reply, order);
         } else {
            handleReplicableCommand(origin, command, reply, order);
         }
      } catch (Throwable t) {
         log.exceptionHandlingCommand(command, t);
         reply.reply(exceptionHandlingCommand(t));
      }
   }

   @Override
   public void handleFromRemoteSite(String origin, XSiteReplicateCommand command, Reply reply, DeliverOrder order) {
      if (trace) {
         log.tracef("Handling command %s from remote site %s", command, origin);
      }

      BackupReceiver receiver = backupReceiverRepository.getBackupReceiver(origin, command.getCacheName().toString());
      if (order.preserveOrder()) {
         runXSiteReplicableCommand(command, receiver, reply);
      } else {
         //the remote site commands may need to be forwarded to the appropriate owners
         remoteCommandsExecutor.execute(() -> runXSiteReplicableCommand(command, receiver, reply));
      }
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
      initializeCacheRpcCommand(command, cr);
      PerCacheInboundInvocationHandler handler = cr.getPerCacheInboundInvocationHandler();
      handler.handle(command, reply, mode);
   }

   private void initializeCacheRpcCommand(CacheRpcCommand command, ComponentRegistry componentRegistry) {
      CommandsFactory commandsFactory = componentRegistry.getCommandsFactory();
      // initialize this command with components specific to the intended cache instance
      commandsFactory.initializeReplicableCommand(command, true);
   }

   private void runXSiteReplicableCommand(XSiteReplicateCommand command, BackupReceiver receiver, Reply reply) {
      try {
         Object returnValue = command.performInLocalSite(receiver);
         reply.reply(SuccessfulResponse.create(returnValue));
      } catch (InterruptedException e) {
         log.shutdownHandlingCommand(command);
         reply.reply(shuttingDownResponse());
      } catch (Throwable throwable) {
         log.exceptionHandlingCommand(command, throwable);
         reply.reply(exceptionHandlingCommand(throwable));
      }
   }

   private void handleReplicableCommand(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
      if (trace) {
         log.tracef("Attempting to execute non-CacheRpcCommand: %s [sender=%s]", command, origin);
      }
      if (order.preserveOrder() || !command.canBlock()) {
         runReplicableCommand(command, reply, order.preserveOrder());
      } else {
         remoteCommandsExecutor.execute(() -> runReplicableCommand(command, reply, order.preserveOrder()));
      }
   }

   private void runReplicableCommand(ReplicableCommand command, Reply reply, boolean preserveOrder) {
      try {
         invokeReplicableCommand(command, reply, preserveOrder);
      } catch (Throwable throwable) {
         if (throwable.getCause() != null && throwable instanceof CompletionException) {
            throwable = throwable.getCause();
         }
         if (throwable instanceof InterruptedException || throwable instanceof IllegalLifecycleStateException) {
            log.shutdownHandlingCommand(command);
            reply.reply(shuttingDownResponse());
         } else {
            log.exceptionHandlingCommand(command, throwable);
            reply.reply(exceptionHandlingCommand(throwable));
         }
      }
   }

   private void invokeReplicableCommand(ReplicableCommand command, Reply reply, boolean preserveOrder)
         throws Throwable {
      globalComponentRegistry.wireDependencies(command);

      CompletableFuture<Object> future = command.invokeAsync();
      if (preserveOrder) {
         future.join();
      } else {
         future.whenComplete((retVal, throwable) -> {
            Response response;
            if (retVal == null || retVal instanceof Response) {
               response = (Response) retVal;
            } else {
               response = SuccessfulResponse.create(retVal);
            }
            reply.reply(response);
         });
      }
   }

}
