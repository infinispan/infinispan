package org.infinispan.remoting.inboundhandler;

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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.BackupReceiverRepository;
import org.infinispan.xsite.XSiteReplicateCommand;

import java.util.concurrent.ExecutorService;

import static org.infinispan.factories.KnownComponentNames.REMOTE_COMMAND_EXECUTOR;

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

   private ExecutorService remoteCommandsExecutor;
   private BackupReceiverRepository backupReceiverRepository;
   private GlobalComponentRegistry globalComponentRegistry;

   public static ExceptionResponse shuttingDownResponse() {
      return new ExceptionResponse(new CacheException("Cache is shutting down"));
   }

   public static ExceptionResponse exceptionHandlingCommand(Throwable throwable) {
      return new ExceptionResponse(new CacheException("Problems invoking command.", throwable));
   }

   @Inject
   public void injectDependencies(@ComponentName(REMOTE_COMMAND_EXECUTOR) ExecutorService remoteCommandsExecutor,
                                  GlobalComponentRegistry globalComponentRegistry,
                                  BackupReceiverRepository backupReceiverRepository) {
      this.remoteCommandsExecutor = remoteCommandsExecutor;
      this.globalComponentRegistry = globalComponentRegistry;
      this.backupReceiverRepository = backupReceiverRepository;
   }

   @Override
   public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
      try {
         if (command instanceof CacheRpcCommand) {
            handleCacheRpcCommand(origin, (CacheRpcCommand) command, reply, order);
         } else {
            if (trace) {
               log.tracef("Attempting to execute non-CacheRpcCommand: %s [sender=%s]", command, origin);
            }
            Runnable runnable = create(command, reply);
            if (order.preserveOrder() || !command.canBlock()) {
               runnable.run();
            } else {
               remoteCommandsExecutor.execute(runnable);
            }
         }
      } catch (Throwable throwable) {
         log.debug(throwable);
         reply.reply(new ExceptionResponse(new CacheException(throwable)));
      }
   }

   @Override
   public void handleFromRemoteSite(String origin, XSiteReplicateCommand command, Reply reply, DeliverOrder order) {
      if (trace) {
         log.tracef("Handling command %s from remote site %s", command, origin);
      }

      BackupReceiver receiver = backupReceiverRepository.getBackupReceiver(origin, command.getCacheName());
      Runnable runnable = create(command, receiver, reply);
      if (order.preserveOrder()) {
         runnable.run();
      } else {
         //the remote site commands may need to be forwarded to the appropriate owners
         remoteCommandsExecutor.execute(runnable);
      }
   }

   private void handleCacheRpcCommand(Address origin, CacheRpcCommand command, Reply reply, DeliverOrder mode) {
      command.setOrigin(origin);
      if (trace) {
         log.tracef("Attempting to execute CacheRpcCommand: %s [sender=%s]", command, origin);
      }
      String cacheName = command.getCacheName();
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

   private Runnable create(final XSiteReplicateCommand command, final BackupReceiver receiver, final Reply reply) {
      return new Runnable() {
         @Override
         public void run() {
            try {
               reply.reply(command.performInLocalSite(receiver));
            } catch (InterruptedException e) {
               log.shutdownHandlingCommand(command);
               reply.reply(shuttingDownResponse());
            } catch (Throwable throwable) {
               log.exceptionHandlingCommand(command, throwable);
               reply.reply(exceptionHandlingCommand(throwable));
            }
         }
      };
   }

   private Runnable create(final ReplicableCommand command, final Reply reply) {
      return new Runnable() {
         @Override
         public void run() {
            try {
               globalComponentRegistry.wireDependencies(command);

               Object retVal = command.perform(null);
               if (retVal != null && !(retVal instanceof Response)) {
                  retVal = SuccessfulResponse.create(retVal);
               }
               reply.reply(retVal);
            } catch (InterruptedException | IllegalLifecycleStateException e) {
               log.shutdownHandlingCommand(command);
               reply.reply(shuttingDownResponse());
            } catch (Throwable throwable) {
               log.exceptionHandlingCommand(command, throwable);
               reply.reply(exceptionHandlingCommand(throwable));
            }
         }
      };
   }
}
