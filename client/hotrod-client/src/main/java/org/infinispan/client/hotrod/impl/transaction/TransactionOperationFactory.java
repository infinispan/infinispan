package org.infinispan.client.hotrod.impl.transaction;

import java.util.concurrent.atomic.AtomicReference;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transaction.operations.CompleteTransactionOperation;
import org.infinispan.client.hotrod.impl.transaction.operations.ForgetTransactionOperation;
import org.infinispan.client.hotrod.impl.transaction.operations.RecoveryOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

/**
 * An operation factory that builds operations independent from the cache used.
 * <p>
 * This operations are the commit/rollback request, forget request and in-doubt transactions request.
 * <p>
 * This operation aren't associated to any cache, but they use the default cache topology to pick the server to
 * contact.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
public class TransactionOperationFactory {

   private final Configuration configuration;
   private final ChannelFactory channelFactory;
   private final Codec codec;
   private final AtomicReference<ClientTopology> clientTopology;

   public TransactionOperationFactory(Configuration configuration, ChannelFactory channelFactory, Codec codec) {
      this.configuration = configuration;
      this.channelFactory = channelFactory;
      this.codec = codec;
      clientTopology = channelFactory.createTopologyId(HotRodConstants.DEFAULT_CACHE_NAME_BYTES);
   }

   CompleteTransactionOperation newCompleteTransactionOperation(Xid xid, boolean commit) {
      return new CompleteTransactionOperation(codec, channelFactory, clientTopology, configuration, xid, commit);
   }

   ForgetTransactionOperation newForgetTransactionOperation(Xid xid) {
      return new ForgetTransactionOperation(codec, channelFactory, clientTopology, configuration, xid);
   }

   RecoveryOperation newRecoveryOperation() {
      return new RecoveryOperation(codec, channelFactory, clientTopology, configuration);
   }

}
