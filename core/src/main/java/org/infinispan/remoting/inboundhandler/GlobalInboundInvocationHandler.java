package org.infinispan.remoting.inboundhandler;

import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.CrossSiteIllegalLifecycleStateException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
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
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.commands.remote.XSiteRequest;

/**
 * {@link org.infinispan.remoting.inboundhandler.InboundInvocationHandler} implementation that handles all the {@link
 * org.infinispan.commands.ReplicableCommand}.
  * This component handles the {@link org.infinispan.commands.ReplicableCommand} from local and remote site. The remote
 * site {@link org.infinispan.commands.ReplicableCommand} are sent to the {@link org.infinispan.xsite.BackupReceiver} to
 * be handled.
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

   // TODO: To be removed with https://issues.redhat.com/browse/ISPN-11483
   @Inject BlockingManager blockingManager;
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
         if (command instanceof HeartBeatCommand) {
            reply.reply(null);
         } else if (command instanceof CacheRpcCommand) {
            handleCacheRpcCommand(origin, (CacheRpcCommand) command, reply, order);
         } else {
            handleReplicableCommand(origin, command, reply, order);
         }
      } catch (Throwable t) {
         if (command.logThrowable(t)) {
            CLUSTER.exceptionHandlingCommand(command, t);
         }
         reply.reply(exceptionHandlingCommand(t));
      }
   }

   @Override
   public void handleFromRemoteSite(String origin, XSiteRequest<?> command, Reply reply, DeliverOrder order) {
      if (log.isTraceEnabled()) {
         log.tracef("Handling command %s from remote site %s", command, origin);
      }
      var rspConsumer = new XSiteResponseConsumer(reply, command);
      if (!globalComponentRegistry.getStatus().allowInvocations()) {
         // Cross-site channel is the first to start.
         // Theoretically, it can receive requests before the CacheManager is ready.
         // return IllegalLifecycleStateException -> triggers back-off and retry in the originator
         rspConsumer.accept(null, log.xsiteCacheManagerDoesNotAllowInvocations(origin));
         return;
      }
      command.invokeInLocalSite(origin, globalComponentRegistry).whenComplete(rspConsumer);
   }

   private void handleCacheRpcCommand(Address origin, CacheRpcCommand command, Reply reply, DeliverOrder mode) {
      if (log.isTraceEnabled()) {
         log.tracef("Attempting to execute CacheRpcCommand: %s [sender=%s]", command, origin);
      }
      ByteString cacheName = command.getCacheName();
      ComponentRegistry cr = globalComponentRegistry.getNamedComponentRegistry(cacheName);

      if (cr == null) {
         if (log.isTraceEnabled()) {
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
      if (log.isTraceEnabled()) {
         log.tracef("Attempting to execute non-CacheRpcCommand: %s [sender=%s]", command, origin);
      }
      Runnable runnable = new ReplicableCommandRunner(command, reply, globalComponentRegistry, order.preserveOrder());
      if (order.preserveOrder() || !command.canBlock()) {
         //we must/can run in this thread
         runnable.run();
      } else {
         blockingManager.runBlocking(runnable, "[blocking] " + command);
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

   private static class ResponseConsumer extends BaseResponseConsumer<ReplicableCommand> {

      final ReplicableCommand command;

      private ResponseConsumer(ReplicableCommand command, Reply reply) {
         super(reply);
         this.command = command;
      }

      @Override
      ReplicableCommand getCommand() {
         return command;
      }

      @Override
      boolean logThrowable(Throwable throwable) {
         return command.logThrowable(throwable);
      }
   }

   private static class XSiteResponseConsumer extends BaseResponseConsumer<XSiteRequest<?>> {

      private final XSiteRequest<?> command;

      private XSiteResponseConsumer(Reply reply, XSiteRequest<?> command) {
         super(reply);
         this.command = command;
      }

      @Override
      XSiteRequest<?> getCommand() {
         return command;
      }

      @Override
      boolean logThrowable(Throwable throwable) {
         return !(throwable instanceof CrossSiteIllegalLifecycleStateException);
      }
   }

   private static abstract class BaseResponseConsumer<T> implements BiConsumer<Object, Throwable> {

      private final Reply reply;

      private BaseResponseConsumer(Reply reply) {
         this.reply = reply;
      }

      @Override
      public void accept(Object retVal, Throwable throwable) {
         reply.reply(convertToResponse(retVal, throwable));
      }

      abstract T getCommand();

      abstract boolean logThrowable(Throwable throwable);

      private Response convertToResponse(Object retVal, Throwable throwable) {
         if (throwable != null) {
            throwable = CompletableFutures.extractException(throwable);
            if (throwable instanceof InterruptedException || throwable instanceof IllegalLifecycleStateException) {
               CLUSTER.debugf("Shutdown while handling command %s", getCommand());
               return shuttingDownResponse();
            } else {
               if (logThrowable(throwable)) {
                  CLUSTER.exceptionHandlingCommand(getCommand(), throwable);
               }
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
