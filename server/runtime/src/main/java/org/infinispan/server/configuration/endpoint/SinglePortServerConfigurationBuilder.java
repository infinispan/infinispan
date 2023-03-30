package org.infinispan.server.configuration.endpoint;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.server.core.configuration.NoAuthenticationConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;
import org.infinispan.server.router.configuration.builder.ConfigurationBuilderParent;
import org.infinispan.server.router.configuration.builder.HotRodRouterBuilder;
import org.infinispan.server.router.configuration.builder.RestRouterBuilder;
import org.infinispan.server.router.configuration.builder.RoutingBuilder;
import org.infinispan.server.router.configuration.builder.SinglePortRouterBuilder;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SinglePortServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<SinglePortRouterConfiguration, SinglePortServerConfigurationBuilder, NoAuthenticationConfiguration> implements ConfigurationBuilderParent {

   private final RoutingBuilder routing = new RoutingBuilder(this);
   private final HotRodRouterBuilder hotRodRouter = new HotRodRouterBuilder(this);
   private final RestRouterBuilder restRouter = new RestRouterBuilder(this);
   private final SinglePortRouterBuilder singlePortRouter = new SinglePortRouterBuilder(this);

   public SinglePortServerConfigurationBuilder() {
      super(HotRodServer.DEFAULT_HOTROD_PORT, SinglePortRouterConfiguration.attributeDefinitionSet());
      singlePortRouter.enabled(true);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public SinglePortServerConfigurationBuilder self() {
      return this;
   }

   @Override
   public SinglePortRouterConfiguration create() {
      return new SinglePortRouterConfiguration(attributes.protect(), ssl.create(), ipFilter.create());
   }

   @Override
   public void validate() {
      super.validate();
   }

   public SinglePortRouterConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public AuthenticationConfigurationBuilder<NoAuthenticationConfiguration> authentication() {
      throw new UnsupportedOperationException();
   }

   @Override
   public SinglePortRouterConfiguration build() {
      return build(true);
   }


   @Override
   public RoutingBuilder routing() {
      return routing;
   }

   @Override
   public HotRodRouterBuilder hotrod() {
      return hotRodRouter;
   }

   @Override
   public RestRouterBuilder rest() {
      return restRouter;
   }

   @Override
   public SinglePortRouterBuilder singlePort() {
      return singlePortRouter;
   }

   public void applyConfigurationToProtocol(ProtocolServerConfigurationBuilder<?, ?, ?> builder) {
      if (attributes.attribute(ProtocolServerConfiguration.HOST).isModified()) {
         builder.host(attributes.attribute(ProtocolServerConfiguration.HOST).get());
      }
      if (attributes.attribute(ProtocolServerConfiguration.PORT).isModified()) {
         builder.port(attributes.attribute(ProtocolServerConfiguration.PORT).get());
      }
      builder.startTransport(false);
   }
}
