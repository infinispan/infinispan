package org.infinispan.server.core.configuration;


/**
 * @author Tristan Tarrant
 * @since 5.3
 */
public class MockServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<MockServerConfiguration, MockServerConfigurationBuilder> {

   public MockServerConfigurationBuilder() {
      super(12345);
   }

   @Override
   public MockServerConfigurationBuilder self() {
      return this;
   }

   @Override
   public MockServerConfiguration build() {
      return build(true);
   }

   public MockServerConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public MockServerConfiguration create() {
      return new MockServerConfiguration(defaultCacheName, name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl.create(), tcpNoDelay, workerThreads);
   }
}
