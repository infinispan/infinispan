package org.horizon.remoting;

import org.horizon.commands.CommandsFactory;
import org.horizon.commands.RPCCommand;
import org.horizon.factories.ComponentRegistry;
import org.horizon.factories.GlobalComponentRegistry;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;
import org.horizon.interceptors.InterceptorChain;
import org.horizon.invocation.InvocationContextContainer;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.statetransfer.StateTransferException;
import org.horizon.statetransfer.StateTransferManager;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Sets the cache interceptor chain on an RPCCommand before calling it to perform
 *
 * @author Manik Surtani
 * @since 1.0
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

   public Object handle(RPCCommand cmd) throws Throwable {
      String cacheName = cmd.getCacheName();
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);
      if (cr == null) {
         if (log.isInfoEnabled()) log.info("Cache named {0} does not exist on this cache manager!", cacheName);
         return null;
      }

      if (!cr.getStatus().allowInvocations()) {
         throw new IllegalStateException("Cache named " + cacheName + " exists but isn't in a state to handle invocations.  Its state is " + cr.getStatus());
      }

      InterceptorChain ic = cr.getLocalComponent(InterceptorChain.class);
      InvocationContextContainer icc = cr.getLocalComponent(InvocationContextContainer.class);
      CommandsFactory commandsFactory = cr.getLocalComponent(CommandsFactory.class);

      cmd.setInterceptorChain(ic);
      // initialize this command with components specific to the intended cache instance
      commandsFactory.initializeReplicableCommand(cmd);
      return cmd.perform(icc.get());
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
