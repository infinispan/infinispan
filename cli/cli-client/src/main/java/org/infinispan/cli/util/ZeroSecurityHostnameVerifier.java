package org.infinispan.cli.util;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ZeroSecurityHostnameVerifier implements HostnameVerifier {
   @Override
   public boolean verify(String hostname, SSLSession session) {
      return true;
   }
}
