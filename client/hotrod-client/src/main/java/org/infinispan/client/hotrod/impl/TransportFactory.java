package org.infinispan.client.hotrod.impl;

import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface TransportFactory {
   public Transport getTransport();

   void init(Properties props);

   void destroy();
}
