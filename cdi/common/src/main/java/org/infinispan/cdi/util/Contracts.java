package org.infinispan.cdi.util;

/**
 * An helper class providing useful assertion methods.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public final class Contracts {
   /**
    * Disable instantiation.
    */
   private Contracts() {
   }

   /**
    * Asserts that the given parameter is not {@code null}.
    *
    * @param param   the parameter to check.
    * @param message the exception message used if the parameter to check is {@code null}.
    * @throws NullPointerException if the given parameter is {@code null}.
    */
   public static void assertNotNull(Object param, String message) {
      if (param == null) {
         throw new NullPointerException(message);
      }
   }
}
