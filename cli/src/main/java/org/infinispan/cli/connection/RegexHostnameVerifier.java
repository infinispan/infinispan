package org.infinispan.cli.connection;

import java.util.regex.Pattern;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

/**
 * @since 13.0
 **/
public class RegexHostnameVerifier implements HostnameVerifier {
   private final Pattern pattern;

   public RegexHostnameVerifier(String pattern) {
      this.pattern = Pattern.compile(pattern);
   }

   @Override
   public boolean verify(String hostname, SSLSession session) {
      return pattern.matcher(hostname).matches();
   }
}
