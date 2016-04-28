package org.infinispan.cli.connection.jmx.remoting;

import static org.infinispan.cli.util.Utils.nullIfEmpty;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.cli.connection.jmx.AbstractJMXUrl;

public abstract class JMXRemotingUrl extends AbstractJMXUrl {

   public JMXRemotingUrl(String connectionString) {
      if (connectionString.length() == 0) {
         hostname = "localhost";
         port = getDefaultPort();
      } else {
         Matcher matcher = getUrlPattern().matcher(connectionString);
         if (!matcher.matches()) {
            throw new IllegalArgumentException(connectionString);
         }
         username = nullIfEmpty(matcher.group(4));
         password = nullIfEmpty(matcher.group(5));
         hostname = nullIfEmpty(matcher.group(6));
         if (matcher.group(7) != null) {
            port = Integer.parseInt(matcher.group(7));
         } else {
            port = getDefaultPort();
         }
         container = nullIfEmpty(matcher.group(8));
         cache = nullIfEmpty(matcher.group(9));
      }
   }

   private Pattern getUrlPattern() {
      return Pattern.compile("^(?:(?![^:@]+:[^:@/]*@)(" + getProtocol() + "):)?(?://)?((?:(([^:@]*):?([^:@]*))?@)?(\\[[0-9A-Fa-f:]+\\]|[^:/?#]*)(?::(\\d*))?)(?:/([^/]*)(?:/(.*))?)?");
   }

   abstract String getProtocol();

   abstract int getDefaultPort();

   @Override
   public String getJMXServiceURL() {
      return "service:jmx:" + getProtocol() + "-jmx://" + hostname + ":" + port;
   }

   @Override
   public String toString() {
      return getProtocol() + "://" + (username == null ? "" : username + "@") + hostname + ":" + port;
   }
}
