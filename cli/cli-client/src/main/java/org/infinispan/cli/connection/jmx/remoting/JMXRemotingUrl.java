package org.infinispan.cli.connection.jmx.remoting;

import static org.infinispan.cli.util.Utils.nullIfEmpty;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.cli.connection.jmx.AbstractJMXUrl;

public class JMXRemotingUrl extends AbstractJMXUrl {
   private static final Pattern JMX_URL = Pattern.compile("^(?:(?![^:@]+:[^:@/]*@)(remoting):)?(?://)?((?:(([^:@]*):?([^:@]*))?@)?(\\[[0-9A-Fa-f:]+\\]|[^:/?#]*)(?::(\\d*))?)(?:/([^/]*)(?:/(.*))?)?");
   private static final int DEFAULT_REMOTING_PORT = 9999;

   public JMXRemotingUrl(String connectionString) {
      if (connectionString.length() == 0) {
         hostname = "localhost";
         port = DEFAULT_REMOTING_PORT;
      } else {
         Matcher matcher = JMX_URL.matcher(connectionString);
         if (!matcher.matches()) {
            throw new IllegalArgumentException(connectionString);
         }
         username = nullIfEmpty(matcher.group(4));
         password = nullIfEmpty(matcher.group(5));
         hostname = nullIfEmpty(matcher.group(6));
         if (matcher.group(7) != null) {
            port = Integer.parseInt(matcher.group(7));
         } else {
            port = DEFAULT_REMOTING_PORT;
         }
         container = nullIfEmpty(matcher.group(8));
         cache = nullIfEmpty(matcher.group(9));
      }
   }

   @Override
   public String getJMXServiceURL() {
      return "service:jmx:remoting-jmx://" + hostname + ":" + port;
   }

   @Override
   public String toString() {
      return "remoting://" + (username == null ? "" : username + "@") + hostname + ":" + port;
   }
}
