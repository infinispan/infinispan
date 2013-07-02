package org.infinispan.cli.connection.jmx.rmi;

import static org.infinispan.cli.util.Utils.nullIfEmpty;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.cli.connection.jmx.AbstractJMXUrl;

public class JMXRMIUrl extends AbstractJMXUrl {
   private static final Pattern JMX_URL = Pattern.compile("^(?:(?![^:@]+:[^:@/]*@)(jmx):)?(?://)?((?:(([^:@]*):?([^:@]*))?@)?(\\[[0-9A-Fa-f:]+\\]|[^:/?#]*)(?::(\\d*))?)(?:/([^/]*)(?:/(.*))?)?");

   public JMXRMIUrl(String connectionString) {
      Matcher matcher = JMX_URL.matcher(connectionString);
      if (!matcher.matches()) {
         throw new IllegalArgumentException(connectionString);
      }
      username = nullIfEmpty(matcher.group(4));
      password = nullIfEmpty(matcher.group(5));
      hostname = nullIfEmpty(matcher.group(6));
      port = Integer.parseInt(matcher.group(7));
      container = nullIfEmpty(matcher.group(8));
      cache = nullIfEmpty(matcher.group(9));
   }

   @Override
   public String getJMXServiceURL() {
      return "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";
   }

   @Override
   public String toString() {
      return "jmx://" + (username == null ? "" : username + "@") + hostname + ":" + port;
   }
}
