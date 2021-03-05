package org.infinispan.commons.marshall;

import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.util.Util;

public class CheckedInputStream extends ObjectInputStream {

   private final ClassAllowList allowList;

   public CheckedInputStream(InputStream in, ClassAllowList allowList) throws IOException {
      super(in);
      this.allowList = allowList;
   }

   @Override
   protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      boolean safeClass = allowList.isSafeClass(desc.getName());
      if (!safeClass)
         throw CONTAINER.classNotInAllowList(desc.getName());
      try {
         return Util.loadClass(desc.getName(), allowList.getClassLoader());
      } catch (Exception e) {
         return super.resolveClass(desc);
      }
   }
}
