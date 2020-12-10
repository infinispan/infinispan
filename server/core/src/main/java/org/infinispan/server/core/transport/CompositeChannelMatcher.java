package org.infinispan.server.core.transport;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelMatcher;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class CompositeChannelMatcher implements ChannelMatcher {
   private final ChannelMatcher[] channelMatchers;

   public CompositeChannelMatcher(ChannelMatcher... channelMatchers) {
      this.channelMatchers = channelMatchers;
   }

   @Override
   public boolean matches(Channel channel) {
      for(ChannelMatcher matcher : channelMatchers) {
         if (!matcher.matches(channel)) {
            return false;
         }
      }
      return true;
   }
}
