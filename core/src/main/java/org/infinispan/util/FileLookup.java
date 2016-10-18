package org.infinispan.util;

import java.io.InputStream;

/**
 * This class is still used by the Hibernate Search integration:
 * will be removed soon.
 *
 * @deprecated Use the new org.infinispan.commons.util.FileLookup
 * @see org.infinispan.commons.util.FileLookup
 */
@Deprecated
public final class FileLookup {

   private final org.infinispan.commons.util.FileLookup fileLookup;

   FileLookup(org.infinispan.commons.util.FileLookup fileLookup) {
      this.fileLookup = fileLookup;
   }

   public InputStream lookupFile(String filename, ClassLoader cl) {
      return fileLookup.lookupFile(filename, cl);
   }

}
