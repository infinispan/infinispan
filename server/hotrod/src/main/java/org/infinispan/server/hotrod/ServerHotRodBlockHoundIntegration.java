package org.infinispan.server.hotrod;

import org.infinispan.commons.util.SaslUtils;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.utils.SslUtils;
import org.infinispan.server.hotrod.tx.PrepareCoordinator;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.kohsuke.MetaInfServices;

import io.netty.util.concurrent.GlobalEventExecutor;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

@MetaInfServices
public class ServerHotRodBlockHoundIntegration implements BlockHoundIntegration {
   @Override
   public void applyTo(BlockHound.Builder builder) {
      builder.allowBlockingCallsInside(GlobalEventExecutor.class.getName(), "addTask");
      builder.allowBlockingCallsInside(GlobalEventExecutor.class.getName(), "takeTask");

      // Invokes stream method on a cache that is REPL or LOCAL without persistence (won't block)
      builder.allowBlockingCallsInside(GlobalTxTable.class.getName(), "getPreparedTransactions");

      questionableBlockingMethod(builder);
   }

   private static void questionableBlockingMethod(BlockHound.Builder builder) {
      // Starting a cache is blocking
      builder.allowBlockingCallsInside(EmbeddedCacheManager.class.getName(), "createCounter");
      builder.allowBlockingCallsInside(HotRodServer.class.getName(), "obtainAnonymizedCache");
      builder.allowBlockingCallsInside(Encoder2x.class.getName(), "getCounterCacheTopology");

      // Size method is blocking when a store is installed
      builder.allowBlockingCallsInside(CacheRequestProcessor.class.getName(), "stats");

      // Stream method is blocking
      builder.allowBlockingCallsInside(Encoder2x.class.getName(), "generateTopologyResponse");

      // Loads a file on ssl connect to read the key store
      builder.allowBlockingCallsInside(SslUtils.class.getName(), "createNettySslContext");

      // Wildfly open ssl reads a properties file
      builder.allowBlockingCallsInside("org.wildfly.openssl.OpenSSLEngine", "unwrap");

      builder.allowBlockingCallsInside(SaslUtils.class.getName(), "getFactories");

      // Blocks on non blocking method - should return CompletionStage
      builder.allowBlockingCallsInside(GlobalTxTable.class.getName(), "update");
      // Uses blocking call inside publisher - can be non blocking instead
      builder.allowBlockingCallsInside(GlobalTxTable.class.getName(), "forgetTransaction");

      // Transaction API is inherently blocking - need to workaround eventually
      builder.allowBlockingCallsInside(PrepareCoordinator.class.getName(), "rollback");
      builder.allowBlockingCallsInside(PrepareCoordinator.class.getName(), "commit");
      builder.allowBlockingCallsInside(TransactionRequestProcessor.class.getName(), "prepareTransactionInternal");

      builder.allowBlockingCallsInside(CounterRequestProcessor.class.getName(), "counterRemoveInternal");
      // Counter creation is blocking - needs to be fixed
      builder.allowBlockingCallsInside(CounterRequestProcessor.class.getName(), "applyCounter");
   }
}
