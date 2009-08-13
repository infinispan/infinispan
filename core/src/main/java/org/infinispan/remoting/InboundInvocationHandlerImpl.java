package org.infinispan.remoting;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.statetransfer.StateTransferException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Sets the cache interceptor chain on an RPCCommand before calling it to perform
 *
 * @author Manik Surtani
 * @since 4.0
 */
@NonVolatile
@Scope(Scopes.GLOBAL)
public class InboundInvocationHandlerImpl implements InboundInvocationHandler {
   GlobalComponentRegistry gcr;
   private static final Log log = LogFactory.getLog(InboundInvocationHandlerImpl.class);

   @Inject
   public void inject(GlobalComponentRegistry gcr) {
      this.gcr = gcr;
   }

   public Response handle(CacheRpcCommand cmd) throws Throwable {
      String cacheName = cmd.getCacheName();
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);
      if (cr == null) {
         if (log.isInfoEnabled()) log.info("Cache named {0} does not exist on this cache manager!", cacheName);
         return null;
      }

      Configuration localConfig = cr.getComponent(Configuration.class);

      if (!cr.getStatus().allowInvocations()) {
         long giveupTime = System.currentTimeMillis() + localConfig.getStateRetrievalTimeout();
         while (cr.getStatus().startingUp() && System.currentTimeMillis() < giveupTime) Thread.sleep(100);
         if (!cr.getStatus().allowInvocations()) {
            log.warn("Cache named [{0}] exists but isn't in a state to handle invocations.  Its state is {1}.", cacheName, cr.getStatus());
            return RequestIgnoredResponse.INSTANCE;
         }
      }

      CommandsFactory commandsFactory = cr.getLocalComponent(CommandsFactory.class);

      // initialize this command with components specific to the intended cache instance
      commandsFactory.initializeReplicableCommand(cmd);

      try {
         log.trace("Calling perform() on {0}", cmd);
         Object retval = cmd.perform(null);
         return cr.getComponent(ResponseGenerator.class).getResponse(cmd, retval);
      } catch (Exception e) {
         return new ExceptionResponse(e);
      }
   }

   public void applyState(String cacheName, InputStream i) throws StateTransferException {
      getStateTransferManager(cacheName).applyState(i);
   }

   public void generateState(String cacheName, OutputStream o) throws StateTransferException {
      getStateTransferManager(cacheName).generateState(o);
   }

   private StateTransferManager getStateTransferManager(String cacheName) throws StateTransferException {
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);
      if (cr == null) {
         String msg = "Cache named " + cacheName + " does not exist on this cache manager!";
         log.info(msg);
         throw new StateTransferException(msg);
      }

      return cr.getComponent(StateTransferManager.class);
   }
}
