package org.infinispan.cli.connection.jmx;

import java.util.Map;

public interface JMXUrl {

   String getJMXServiceURL();

   String getContainer();

   String getCache();

   Map<String, Object> getConnectionEnvironment(String credentials);

   boolean needsCredentials();

}
