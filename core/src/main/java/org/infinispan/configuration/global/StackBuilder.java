package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;

public interface StackBuilder<T> extends Builder<T> {

   JGroupsChannelConfigurator getConfigurator();
}
