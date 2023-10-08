package org.infinispan.commands.remote;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;

/**
 * A {@link CacheRpcCommand} that returns a {@link Collection} of local {@link GlobalTransaction} already completed.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CHECK_TRANSACTION_RPC_COMMAND)
public class CheckTransactionRpcCommand implements CacheRpcCommand {

   public static final int COMMAND_ID = 83;
   private static final ResponseCollectorImpl INSTANCE = new ResponseCollectorImpl();

   private final ByteString cacheName;
   private Collection<GlobalTransaction> gtxToCheck;

   public static ResponseCollector<Collection<GlobalTransaction>> responseCollector() {
      return INSTANCE;
   }

   public CheckTransactionRpcCommand(ByteString cacheName, Collection<GlobalTransaction> gtxToCheck) {
      this.cacheName = cacheName;
      this.gtxToCheck = gtxToCheck;
   }

   @ProtoFactory
   CheckTransactionRpcCommand(ByteString cacheName, MarshallableCollection<GlobalTransaction> transactions) {
      this.cacheName = cacheName;
      this.gtxToCheck = MarshallableCollection.unwrap(transactions);
   }

   @Override
   @ProtoField(number = 1)
   public ByteString getCacheName() {
      return cacheName;
   }

   @ProtoField(number = 2)
   MarshallableCollection<GlobalTransaction> getTransactions() {
      return MarshallableCollection.create(gtxToCheck);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) {
      // Modify the collection destructively and return the list of completed transactions.
      TransactionTable txTable = componentRegistry.getTransactionTable();
      gtxToCheck.removeIf(txTable::containsLocalTx);
      return CompletableFuture.completedFuture(gtxToCheck);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public Address getOrigin() {
      //we don't need to keep track who sent the message
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //we don't need to keep track who sent the message
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "CheckTransactionRpcCommand{" +
             "cacheName=" + cacheName +
             ", gtxToCheck=" + gtxToCheck +
             '}';
   }

   /**
    * The {@link ResponseCollector} implementation for this command.
    * <p>
    * It ignores all the exceptions and convert them to {@link Collections#emptyList()}.
    */
   private static class ResponseCollectorImpl extends ValidSingleResponseCollector<Collection<GlobalTransaction>> {

      @Override
      protected Collection<GlobalTransaction> withValidResponse(Address sender, ValidResponse response) {
         if (response instanceof SuccessfulResponse) {
            return ((SuccessfulResponse) response).getResponseCollection();
         } else {
            return Collections.emptyList();
         }
      }

      @Override
      protected Collection<GlobalTransaction> targetNotFound(Address sender) {
         //ignore exceptions
         return Collections.emptyList();
      }

      @Override
      protected Collection<GlobalTransaction> withException(Address sender, Exception exception) {
         //ignore exceptions
         return Collections.emptyList();
      }
   }
}
