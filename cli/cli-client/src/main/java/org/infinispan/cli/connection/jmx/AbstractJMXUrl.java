package org.infinispan.cli.connection.jmx;

import java.util.HashMap;
import java.util.Map;

import javax.management.remote.JMXConnector;

public abstract class AbstractJMXUrl implements JMXUrl {
   protected String hostname;
   protected int port;
   protected String username;
   protected String password;
   protected String container;
   protected String cache;

   @Override
   public String getContainer() {
      return container;
   }

   @Override
   public String getCache() {
      return cache;
   }

   @Override
   public boolean needsCredentials() {
      return username != null && password == null;
   }

   @Override
   public Map<String, Object> getConnectionEnvironment(String credentials) {
      Map<String, Object> env = new HashMap<String, Object>();
      if (username != null) {
         env.put(JMXConnector.CREDENTIALS, new String[] { username, credentials != null ? credentials : password });
      }
      return env;
   }

}
