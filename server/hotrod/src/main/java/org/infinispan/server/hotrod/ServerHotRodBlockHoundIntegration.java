package org.infinispan.server.hotrod;

import org.infinispan.commons.util.SaslUtils;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.kohsuke.MetaInfServices;

import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class ServerHotRodBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      // Invokes stream method on a cache that is REPL or LOCAL without persistence (won't block)
      builder.allowBlockingCallsInside(GlobalTxTable.class.getName(), "getPreparedTransactions");

      questionableBlockingMethod(builder);
   }

   private static void questionableBlockingMethod(BlockHound.Builder builder) {
      builder.allowBlockingCallsInside(HotRodServer.class.getName(), "obtainAnonymizedCache");
      builder.allowBlockingCallsInside(Encoder2x.class.getName(), "getCounterCacheTopology");

      // Stream method is blocking
      builder.allowBlockingCallsInside(Encoder2x.class.getName(), "generateTopologyResponse");

      // Wildfly open ssl reads a properties file
      builder.allowBlockingCallsInside("org.wildfly.openssl.OpenSSLEngine", "unwrap");

      builder.allowBlockingCallsInside(SaslUtils.class.getName(), "getFactories");

      // Blocks on non blocking method - should return CompletionStage
      builder.allowBlockingCallsInside(GlobalTxTable.class.getName(), "update");
      // Uses blocking call inside publisher - can be non blocking instead
      builder.allowBlockingCallsInside(GlobalTxTable.class.getName(), "forgetTransaction");

      builder.allowBlockingCallsInside(CounterRequestProcessor.class.getName(), "counterRemoveInternal");
      // Counter creation is blocking - needs to be fixed
      builder.allowBlockingCallsInside(CounterRequestProcessor.class.getName(), "applyCounter");
   }
}
