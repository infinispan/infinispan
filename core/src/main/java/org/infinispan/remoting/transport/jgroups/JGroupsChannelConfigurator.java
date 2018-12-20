package org.infinispan.remoting.transport.jgroups;

import org.jgroups.JChannel;
import org.jgroups.conf.ProtocolStackConfigurator;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface JGroupsChannelConfigurator extends ProtocolStackConfigurator {
   String getName();

   JChannel createChannel() throws Exception;
}
