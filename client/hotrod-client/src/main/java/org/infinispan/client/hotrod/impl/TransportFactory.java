package org.infinispan.client.hotrod.impl;

import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface TransportFactory {

   public static final String CONF_HOTROD_SERVERS = "hotrod-servers";

   public static final String OVERRIDE_HOTROD_SERVERS = "infinispan.hotrod-client.servers-default";

   public Transport getTransport();

   void init(Properties props);

   void destroy();
}
