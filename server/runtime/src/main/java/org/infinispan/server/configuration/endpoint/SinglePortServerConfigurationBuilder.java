package org.infinispan.server.configuration.endpoint;

import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;
import org.infinispan.server.router.configuration.builder.ConfigurationBuilderParent;
import org.infinispan.server.router.configuration.builder.HotRodRouterBuilder;
import org.infinispan.server.router.configuration.builder.RestRouterBuilder;
import org.infinispan.server.router.configuration.builder.RoutingBuilder;
import org.infinispan.server.router.configuration.builder.SinglePortRouterBuilder;
import org.infinispan.server.security.ServerSecurityRealm;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SinglePortServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<SinglePortRouterConfiguration, SinglePortServerConfigurationBuilder> implements ConfigurationBuilderParent {

   private RoutingBuilder routing = new RoutingBuilder(this);
   private HotRodRouterBuilder hotRodRouter = new HotRodRouterBuilder(this);
   private RestRouterBuilder restRouter = new RestRouterBuilder(this);
   private SinglePortRouterBuilder singlePortRouter = new SinglePortRouterBuilder(this);
   private ServerSecurityRealm securityRealm;

   public SinglePortServerConfigurationBuilder() {
      super(HotRodServer.DEFAULT_HOTROD_PORT, SinglePortRouterConfiguration.attributeDefinitionSet());
      singlePortRouter.enabled(true);
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

   public SinglePortServerConfigurationBuilder securityRealm(ServerSecurityRealm securityRealm) {
      this.securityRealm = securityRealm;
      return this;
   }

   public ServerSecurityRealm securityRealm() {
      return securityRealm;
   }

   public void applyConfigurationToProtocol(ProtocolServerConfigurationBuilder builder) {
      if (attributes.attribute(ProtocolServerConfiguration.HOST).isModified()) {
         builder.host(attributes.attribute(ProtocolServerConfiguration.HOST).get());
      }
      if (attributes.attribute(ProtocolServerConfiguration.PORT).isModified()) {
         builder.port(attributes.attribute(ProtocolServerConfiguration.PORT).get());
      }
      builder.startTransport(false);
   }
}
