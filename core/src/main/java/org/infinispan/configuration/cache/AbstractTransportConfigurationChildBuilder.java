package org.infinispan.configuration.cache;

public abstract class AbstractTransportConfigurationChildBuilder<T> extends AbstractConfigurationChildBuilder<T> implements TransactionConfigurationChildBuilder {

   private final TransactionConfigurationBuilder transactionConfigurationBuilder;
   
   protected AbstractTransportConfigurationChildBuilder(TransactionConfigurationBuilder builder) {
      super(builder.getBuilder());
      this.transactionConfigurationBuilder = builder;
   }
   
   @Override
   public RecoveryConfigurationBuilder recovery() {
      return transactionConfigurationBuilder.recovery();
   }

}
