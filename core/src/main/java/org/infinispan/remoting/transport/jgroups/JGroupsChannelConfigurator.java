package org.infinispan.remoting.transport.jgroups;

import javax.sql.DataSource;

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

   void setDataSource(DataSource dataSource);

   void addChannelListener(ChannelListener listener);
}
