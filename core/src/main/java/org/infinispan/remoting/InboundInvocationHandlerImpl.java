package org.infinispan.remoting;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.config.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.invocation.InvocationContextContainer;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;
import org.infinispan.statetransfer.StateTransferException;
import org.infinispan.statetransfer.StateTransferManager;

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

   public Object handle(CacheRpcCommand cmd) throws Throwable {
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
         if (!cr.getStatus().allowInvocations())
            throw new IllegalStateException("Cache named " + cacheName + " exists but isn't in a state to handle invocations.  Its state is " + cr.getStatus());
      }

      InvocationContextContainer icc = cr.getLocalComponent(InvocationContextContainer.class);
      CommandsFactory commandsFactory = cr.getLocalComponent(CommandsFactory.class);

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
