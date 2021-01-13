package org.infinispan.remoting.transport.jgroups;


import org.jgroups.JChannel;
import org.jgroups.stack.Protocol;
import org.jgroups.util.SocketFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public abstract class AbstractJGroupsChannelConfigurator implements JGroupsChannelConfigurator {
   private SocketFactory socketFactory;

   @Override
   public void setSocketFactory(SocketFactory socketFactory) {
      this.socketFactory = socketFactory;
   }

   public SocketFactory getSocketFactory() {
      return socketFactory;
   }

   protected JChannel applySocketFactory(JChannel channel) {
      if (socketFactory != null) {
         Protocol protocol = channel.getProtocolStack().getTopProtocol();
         protocol.setSocketFactory(socketFactory);
      }
      return channel;
   }
}
