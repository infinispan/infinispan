package org.infinispan.util;

import java.io.InputStream;

/**
 * This class is still used by the Hibernate Search integration:
 * will be removed soon.
 *
 * @deprecated Use the new org.infinispan.commons.util.Util
 * @see org.infinispan.commons.util.Util
 */
@Deprecated
public final class Util {

   private Util() {
      //not allowed to create instances
   }

   public static void close(InputStream is) {
      org.infinispan.commons.util.Util.close(is);
   }

}
