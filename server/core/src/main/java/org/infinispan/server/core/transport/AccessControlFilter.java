package org.infinispan.server.core.transport;

import java.net.InetSocketAddress;

import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.logging.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ipfilter.IpFilterRule;
import io.netty.handler.ipfilter.IpFilterRuleType;

/**
 * This class provides the functionality to either accept or reject new {@link Channel}s based on their IP address.
 *
 * @since 12.1
 */
@Sharable
public class AccessControlFilter<A extends ProtocolServerConfiguration> extends ChannelInboundHandlerAdapter {
   public static final AccessControlFilterEvent EVENT = new AccessControlFilterEvent();
   private final A configuration;
   private final boolean onRegistration;

   public AccessControlFilter(A configuration) {
      this(configuration, true);
   }

   public AccessControlFilter(A configuration, boolean onRegistration) {
      this.configuration = configuration;
      this.onRegistration = onRegistration;
   }

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof AccessControlFilterEvent) {
         processRules(ctx);
      }
      ctx.fireUserEventTriggered(evt);
   }

   protected boolean accept(InetSocketAddress remoteAddress) throws Exception {
      if (!configuration.isEnabled()) {
         return false;
      }
      for (IpFilterRule rule : configuration.ipFilter().rules()) {
         if (rule.matches(remoteAddress)) {
            if (rule.ruleType() == IpFilterRuleType.REJECT) {
               Log.SECURITY.ipFilterConnectionRejection(remoteAddress, rule);
            }
            return rule.ruleType() == IpFilterRuleType.ACCEPT;
         }
      }
      return true;
   }

   @Override
   public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      if (onRegistration) {
         processRules(ctx);
      }
      ctx.fireChannelRegistered();
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      if (!processRules(ctx)) {
         throw new IllegalStateException("cannot determine to accept or reject a channel: " + ctx.channel());
      } else {
         ctx.fireChannelActive();
      }
   }

   private boolean processRules(ChannelHandlerContext ctx) throws Exception {
      InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();

      // If the remote address is not available yet, defer the decision.
      if (remoteAddress == null) {
         return false;
      }

      // No need to keep this handler in the pipeline anymore because the decision is going to be made now.
      // Also, this will prevent the subsequent events from being handled by this handler.
      ctx.pipeline().remove(this);

      if (!accept(remoteAddress)) {
         ctx.close();
      }

      return true;
   }

   public static class AccessControlFilterEvent {
      private AccessControlFilterEvent() {}
   }

}
