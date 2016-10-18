package org.infinispan.util;

/**
 * This class is still used by the Hibernate Search integration:
 * will be removed soon.
 *
 * @deprecated Use the new org.infinispan.commons.util.FileLookupFactory
 * @see org.infinispan.commons.util.FileLookupFactory
 */
@Deprecated
public class FileLookupFactory {

   private FileLookupFactory() {
      //not allowed to create instances
   }

   public static FileLookup newInstance() {
      org.infinispan.commons.util.FileLookup fileLookup = org.infinispan.commons.util.FileLookupFactory.newInstance();
      return new FileLookup(fileLookup);
   }

}
