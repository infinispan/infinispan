package org.infinispan.commons.jdkspecific;

import javax.net.ssl.SSLParameters;

import org.infinispan.commons.logging.Log;

public class SSLParametersHelper {
   public static void setNamedGroups(SSLParameters parameters, String[] namedGroups) {
      Log.CONTAINER.sslNamedGroupsUnsupported();
   }

   public static boolean isNamedGroupAvailable(String namedGroup) {
      return false;
   }
}
