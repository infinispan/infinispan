package org.infinispan.remoting.inboundhandler;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;

import java.util.Objects;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.xsite.commands.remote.XSiteRequest;

/**
 * A {@link InboundInvocationHandler} that offloads the request execution to the {@link BlockingManager}.
 * <p>
 * Use it when the test requires the request to process, and you do not want to block the JGroups message batch
 * processing.
 */
@Scope(Scopes.GLOBAL)
public class OffloadInboundInvocationHandler implements InboundInvocationHandler {

   private final InboundInvocationHandler delegate;
   @Inject
   BlockingManager blockingManager;

   public static void replaceOn(EmbeddedCacheManager manager) {
      var iih = extractGlobalComponent(manager, InboundInvocationHandler.class);
      if (iih instanceof OffloadInboundInvocationHandler) {
         return;
      }
      wrapGlobalComponent(manager, InboundInvocationHandler.class, OffloadInboundInvocationHandler::new, true);
   }

   private OffloadInboundInvocationHandler(InboundInvocationHandler delegate) {
      this.delegate = Objects.requireNonNull(delegate);
   }

   @Override
   public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
      blockingManager.runBlocking(() -> delegate.handleFromCluster(origin, command, reply, order), "offload-inbound-handler-cluster");
   }

   @Override
   public void handleFromRemoteSite(String origin, XSiteRequest<?> command, Reply reply, DeliverOrder order) {
      blockingManager.runBlocking(() -> delegate.handleFromRemoteSite(origin, command, reply, order), "offload-inbound-handler-site");
   }
}
