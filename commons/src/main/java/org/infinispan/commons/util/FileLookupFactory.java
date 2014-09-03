package org.infinispan.commons.util;

/**
 * @deprecated This class been deprecated and will be removed in future
 * versions. Alternatively, instantiate {@link org.infinispan.commons.util.FileLookup}
 * class directly.
 */
@Deprecated
public class FileLookupFactory {
   public static FileLookup newInstance() {
      return new FileLookup();
   }
}