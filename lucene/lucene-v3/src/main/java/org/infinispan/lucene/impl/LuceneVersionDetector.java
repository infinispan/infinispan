package org.infinispan.lucene.impl;

import org.infinispan.lucene.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * Since Lucene requires extension of Directory (it's not an interface)
 * we need to apply some tricks to provide the correct Directory implementation
 * depending on the Lucene version detected on the classpath.
 *
 * @since 5.2
 * @author Sanne Grinovero
 */
public class LuceneVersionDetector {

   public static final int VERSION = detectVersion();

   private LuceneVersionDetector() {
      //Not to be instantiated
   }

   private static int detectVersion() {
      Log log = LogFactory.getLog(LuceneVersionDetector.class, Log.class);
      int version = 3;
      try {
         Class.forName("org.apache.lucene.store.IOContext", true, LuceneVersionDetector.class.getClassLoader());
         version = 4;
      }
      catch (ClassNotFoundException e) {
      }
      log.detectedLuceneVersion(version);
      return version;
   }

}
