package org.infinispan.remoting;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.CacheManager;
import org.infinispan.marshall.Marshaller;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.statetransfer.StateTransferException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * Sets the cache interceptor chain on an RPCCommand before calling it to perform
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class InboundInvocationHandlerImpl implements InboundInvocationHandler {
   GlobalComponentRegistry gcr;
   private static final Log log = LogFactory.getLog(InboundInvocationHandlerImpl.class);
   private Marshaller marshaller;

   @Inject
   public void inject(GlobalComponentRegistry gcr, Marshaller marshaller) {
      this.gcr = gcr;
      this.marshaller = marshaller;
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
      StateTransferManager manager = getStateTransferManager(cacheName);
      if (manager == null) {
         ObjectOutput oo = null;
         try {
            oo = marshaller.startObjectOutput(o, false);
            // Not started yet, so send started flag false
            marshaller.objectToObjectStream(false, oo);
         } catch (Exception e) {
            throw new StateTransferException(e);
         } finally {
            marshaller.finishObjectOutput(oo);
         }
      } else {
         manager.generateState(o);
      }
   }

   private StateTransferManager getStateTransferManager(String cacheName) throws StateTransferException {
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);
      if (cr == null)
         return null;
      return cr.getComponent(StateTransferManager.class);
   }
}
