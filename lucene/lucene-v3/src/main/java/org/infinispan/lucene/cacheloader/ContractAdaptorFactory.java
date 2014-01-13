package org.infinispan.lucene.cacheloader;

import org.apache.lucene.store.Directory;

/**
 * @since 5.2
 * @author Sanne Grinovero
 */
public class ContractAdaptorFactory {

   private ContractAdaptorFactory() {
      //not to be created
   }

   public static InternalDirectoryContract wrapNativeDirectory(Directory directory) {
      return new DirectoryV4Adaptor(directory);
   }

}
