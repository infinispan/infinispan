package org.infinispan.client.hotrod.impl.transport.tcp;

import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 * @deprecated since 9.2, implementation class, retained only as {@link TransportFactory} leaks the {@link ClusterSwitchStatus}
 */
public class TcpTransportFactory {
   @Deprecated
   public enum ClusterSwitchStatus {
      NOT_SWITCHED, SWITCHED, IN_PROGRESS;
   }
}
