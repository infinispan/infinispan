package org.infinispan.server.core.iteration;

import org.infinispan.server.core.transport.OnChannelCloseReaper;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface IterationState {
   String getId();

   OnChannelCloseReaper getReaper();
}
