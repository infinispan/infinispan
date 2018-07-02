package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

public class CheckedInputStream extends ObjectInputStream {

   private static final Log log = LogFactory.getLog(CheckedInputStream.class, Log.class);

   private final ClassWhiteList whitelist;

   public CheckedInputStream(InputStream in, ClassWhiteList whitelist) throws IOException {
      super(in);
      this.whitelist = whitelist;
   }

   @Override
   protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      boolean safeClass = whitelist.isSafeClass(desc.getName());
      if (!safeClass)
         throw log.classNotInWhitelist(desc.getName());

      return super.resolveClass(desc);
   }
}
