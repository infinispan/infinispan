package org.infinispan.remoting.transport.jgroups;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.jgroups.ChannelListener;
import org.jgroups.JChannel;
import org.jgroups.stack.Protocol;
import org.jgroups.util.SocketFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public abstract class AbstractJGroupsChannelConfigurator implements JGroupsChannelConfigurator {
   private SocketFactory socketFactory;
   protected List<ChannelListener> channelListeners = new ArrayList<>(2);
   protected Executor executor;

   @Override
   public void setSocketFactory(SocketFactory socketFactory) {
      this.socketFactory = socketFactory;
   }

   public SocketFactory getSocketFactory() {
      return socketFactory;
   }

   protected JChannel amendChannel(JChannel channel) {
      if (socketFactory != null) {
         Protocol protocol = channel.getProtocolStack().getTopProtocol();
         protocol.setSocketFactory(socketFactory);
      }
      for(ChannelListener listener : channelListeners) {
         channel.addChannelListener(listener);
      }
      if (executor != null) {
         channel.getProtocolStack().getTransport().setThreadPool(executor);
      }
      return channel;
   }

   public void addChannelListener(ChannelListener channelListener) {
      channelListeners.add(channelListener);
   }

   @Override
   public void setExecutor(Executor executor) {
      this.executor = executor;
   }
}
