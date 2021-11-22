package org.infinispan.configuration.global;

import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;

/**
 * @since 14.0
 **/
public interface NamedStackConfiguration {
   String name();
   JGroupsChannelConfigurator configurator();
}
