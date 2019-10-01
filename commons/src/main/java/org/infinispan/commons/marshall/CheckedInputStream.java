package org.infinispan.commons.marshall;

import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import org.infinispan.commons.configuration.ClassWhiteList;

public class CheckedInputStream extends ObjectInputStream {

   private final ClassWhiteList whitelist;

   public CheckedInputStream(InputStream in, ClassWhiteList whitelist) throws IOException {
      super(in);
      this.whitelist = whitelist;
   }

   @Override
   protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      boolean safeClass = whitelist.isSafeClass(desc.getName());
      if (!safeClass)
         throw CONTAINER.classNotInWhitelist(desc.getName());

      return super.resolveClass(desc);
   }
}
