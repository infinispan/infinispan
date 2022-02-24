package org.infinispan.remoting.transport.jgroups;

import org.jgroups.ChannelListener;
import org.jgroups.JChannel;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.util.SocketFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface JGroupsChannelConfigurator extends ProtocolStackConfigurator {
   String getName();

   JChannel createChannel(String name) throws Exception;

   void setSocketFactory(SocketFactory socketFactory);

   void addChannelListener(ChannelListener listener);
}
