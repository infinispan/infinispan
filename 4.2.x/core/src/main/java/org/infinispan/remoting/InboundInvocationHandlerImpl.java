package org.infinispan.remoting;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.marshall.StreamingMarshaller;
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
   private StreamingMarshaller marshaller;
   private EmbeddedCacheManager embeddedCacheManager;

   @Inject
   public void inject(GlobalComponentRegistry gcr, StreamingMarshaller marshaller, EmbeddedCacheManager embeddedCacheManager) {
      this.gcr = gcr;
      this.marshaller = marshaller;
      this.embeddedCacheManager = embeddedCacheManager;
   }

   private boolean isDefined(String cacheName) {
      log.error("Defined caches: {0}", embeddedCacheManager.getCacheNames());
      return embeddedCacheManager.getCacheNames().contains(cacheName);
   }

   public Response handle(CacheRpcCommand cmd) throws Throwable {
      String cacheName = cmd.getCacheName();
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);

      if (cr == null) {
         // lets see if the cache is *defined* and perhaps just not started.
         if (isDefined(cacheName)) {
            log.info("Will trya nd wait for the cache to start");
            long giveupTime = System.currentTimeMillis() + 30000; // arbitraty (?) wait time for caches to start
            while (cr == null && System.currentTimeMillis() < giveupTime) {
               Thread.sleep(100);
               cr = gcr.getNamedComponentRegistry(cacheName);
            }
         } else {
            log.info("Cache {0} is not defined.  No point in waiting.", cacheName);
         }

         if (cr == null) {
            if (log.isInfoEnabled()) log.info("Cache named {0} does not exist on this cache manager!", cacheName);
            return new ExceptionResponse(new NamedCacheNotFoundException(cacheName));
         }
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
      commandsFactory.initializeReplicableCommand(cmd, true);

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
