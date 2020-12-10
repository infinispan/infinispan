package org.infinispan.server.router.router.impl.singleport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.rest.ALPNHandler;
import org.infinispan.rest.RestServer;
import org.infinispan.server.core.ProtocolServer;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;

/**
 * Netty upgrade handler for Single Port
 *
 * @author Sebastian Łaskawiec
 */
public class SinglePortUpgradeHandler extends ALPNHandler {

   private final boolean useAlpn;
   private final Map<String, ProtocolServer> upgradeServers;

   public SinglePortUpgradeHandler(boolean useAlpn, RestServer restServer, Map<String, ProtocolServer> upgradeServers) {
      super(restServer);
      this.useAlpn = useAlpn;
      this.upgradeServers = upgradeServers;
   }

   @Override
   protected ProtocolServer<?> getProtocolServer(String protocol) {
      return upgradeServers.get(protocol);
   }

   @Override
   public ApplicationProtocolConfig getAlpnConfiguration() {
      if (useAlpn) {
         List<String> supportedProtocols = new ArrayList<>();
         supportedProtocols.add(ApplicationProtocolNames.HTTP_2);
         supportedProtocols.add(ApplicationProtocolNames.HTTP_1_1);
         supportedProtocols.addAll(upgradeServers.keySet());

         return new ApplicationProtocolConfig(
               ApplicationProtocolConfig.Protocol.ALPN,
               // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
               ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
               // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
               ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
               supportedProtocols);
      }
      return null;
   }

   public Map<String, ProtocolServer> getUpgradeServers() {
      return upgradeServers;
   }
}
