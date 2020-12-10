package org.infinispan.server.core.transport;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelMatcher;
import io.netty.handler.ipfilter.IpFilterRule;
import io.netty.handler.ipfilter.IpFilterRuleType;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class IpFilterRuleChannelMatcher implements ChannelMatcher {
   private final Iterable<? extends IpFilterRule> rules;

   public IpFilterRuleChannelMatcher(Iterable<? extends IpFilterRule> rules) {
      this.rules = rules;
   }

   @Override
   public boolean matches(Channel channel) {
      for (IpFilterRule rule : rules) {
         if (rule.matches((InetSocketAddress) channel.remoteAddress())) {
            return rule.ruleType() == IpFilterRuleType.REJECT;
         }
      }
      return false;
   }
}
