package org.infinispan.lucene.cacheloader;

import org.apache.lucene.store.Directory;
import org.infinispan.lucene.impl.LuceneVersionDetector;
import org.infinispan.lucene.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @since 5.2
 * @author Sanne Grinovero
 */
public class ContractAdaptorFactory {

   private static final Log log = LogFactory.getLog(ContractAdaptorFactory.class, Log.class);

   private ContractAdaptorFactory() {
      //not to be created
   }


   public static InternalDirectoryContract wrapNativeDirectory(Directory directory) {
      if (LuceneVersionDetector.VERSION == 3) {
         return new DirectoryV3Adaptor(directory);
      }
      else {
         Class<?>[] ctorType = new Class[]{ Directory.class };
         InternalDirectoryContract idc;
         try {
            idc = (InternalDirectoryContract) ContractAdaptorFactory.class.getClassLoader()
               .loadClass("org.infinispan.lucene.cacheloader.DirectoryV4Adaptor")
               .getConstructor(ctorType)
               .newInstance(directory);
         }
         catch (Exception e) {
            throw log.failedToCreateLucene4Directory(e);
         }
         return idc;
      }
   }

}
