package org.infinispan.commons.jdkspecific;

import java.util.Arrays;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

public class SSLParametersHelper {
   public static void setNamedGroups(SSLParameters parameters, String[] namedGroups) {
      parameters.setNamedGroups(namedGroups);
   }

   public static boolean isNamedGroupAvailable(String namedGroup) {
      try {
         SSLContext ctx = SSLContext.getInstance("TLSv1.3");
         ctx.init(null, null, null);
         String[] groups = ctx.getSupportedSSLParameters().getNamedGroups();
         return groups != null && Arrays.asList(groups).contains(namedGroup);
      } catch (Exception e) {
         return false;
      }
   }
}
