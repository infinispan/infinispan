package org.infinispan.server.router.router.impl.singleport;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.rest.Http11To2UpgradeHandler;
import org.infinispan.rest.RestServer;
import org.infinispan.server.core.ProtocolServer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;

/**
 * Netty upgrade handler for Single Port
 *
 * @author Sebastian Łaskawiec
 */
public class SinglePortUpgradeHandler extends Http11To2UpgradeHandler {

   private final boolean useAlpn;
   private final Map<String, ProtocolServer> upgradeServers;

   public SinglePortUpgradeHandler(boolean useAlpn, RestServer restServer, Map<String, ProtocolServer> upgradeServers) {
      super(restServer);
      this.useAlpn = useAlpn;
      this.upgradeServers = upgradeServers;
   }

   public void configurePipeline(ChannelPipeline pipeline, String protocol) {
      if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
         configureHttp2(pipeline);
         return;
      }

      if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
         configureHttp1(pipeline);
         return;
      }

      ProtocolServer protocolServer = upgradeServers.get(protocol);
      if (protocolServer != null) {
         pipeline.addLast(protocolServer.getInitializer());
         return;
      }

      throw new IllegalStateException("unknown protocol: " + protocol);
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

   @Override
   protected HttpServerUpgradeHandler.UpgradeCodec upgradeCodecForHttp11(CharSequence protocol) {
      // Let's check if it's HTTP/2 - parent handler know how to upgrade to that - it's REST after all.
      HttpServerUpgradeHandler.UpgradeCodec upgradeCodec = super.upgradeCodecForHttp11(protocol);
      if (upgradeCodec == null) {
         // Not HTTP/2 switch, maybe it's a custom protocol server?
         if (upgradeServers.containsKey(protocol)) {
            ProtocolServer protocolServer = upgradeServers.get(protocol);
            upgradeCodec = new HttpServerUpgradeHandler.UpgradeCodec() {
               @Override
               public Collection<CharSequence> requiredUpgradeHeaders() {
                  return Collections.emptyList();
               }

               @Override
               public boolean prepareUpgradeResponse(ChannelHandlerContext ctx, FullHttpRequest upgradeRequest, HttpHeaders upgradeHeaders) {
                  return true;
               }

               @Override
               public void upgradeTo(ChannelHandlerContext ctx, FullHttpRequest upgradeRequest) {
                  ctx.pipeline().addLast(protocolServer.getInitializer());
               }
            };
         }
      }
      // If at this point, this is null, we don't understand the target protocol, let's keep HTTP/1.1
      return upgradeCodec;
   }
}
