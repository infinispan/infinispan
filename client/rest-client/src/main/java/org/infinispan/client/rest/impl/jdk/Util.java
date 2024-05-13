package org.infinispan.client.rest.impl.jdk;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

final class Util {
   static String sanitize(String s) {
      return URLEncoder.encode(s, StandardCharsets.UTF_8);
   }
}
