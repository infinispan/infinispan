package org.infinispan.server.hotrod.configuration;

/**
 * AbstractHotRodServerChildConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public abstract class AbstractHotRodServerChildConfigurationBuilder implements HotRodServerChildConfigurationBuilder {
   private final HotRodServerChildConfigurationBuilder builder;

   protected AbstractHotRodServerChildConfigurationBuilder(HotRodServerChildConfigurationBuilder builder) {
      this.builder = builder;
   }

   @Override
   public AuthenticationConfigurationBuilder authentication() {
      return builder.authentication();
   }

   @Override
   public HotRodServerChildConfigurationBuilder proxyHost(String proxyHost) {
      return builder.proxyHost(proxyHost);
   }

   @Override
   public HotRodServerChildConfigurationBuilder proxyPort(int proxyPort) {
      return builder.proxyPort(proxyPort);
   }

   @Override
   public HotRodServerChildConfigurationBuilder topologyLockTimeout(long topologyLockTimeout) {
      return builder.topologyLockTimeout(topologyLockTimeout);
   }

   @Override
   public HotRodServerChildConfigurationBuilder topologyReplTimeout(long topologyReplTimeout) {
      return builder.topologyReplTimeout(topologyReplTimeout);
   }

   @Override
   public HotRodServerChildConfigurationBuilder topologyAwaitInitialTransfer(boolean topologyAwaitInitialTransfer) {
      return builder.topologyAwaitInitialTransfer(topologyAwaitInitialTransfer);
   }

   @Override
   public HotRodServerChildConfigurationBuilder topologyStateTransfer(boolean topologyStateTransfer) {
      return builder.topologyStateTransfer(topologyStateTransfer);
   }

}
