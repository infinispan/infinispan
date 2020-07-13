package org.infinispan.commons.marshall;

import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import org.infinispan.commons.configuration.ClassAllowList;

public class CheckedInputStream extends ObjectInputStream {

   private final ClassAllowList whitelist;

   public CheckedInputStream(InputStream in, ClassAllowList whitelist) throws IOException {
      super(in);
      this.whitelist = whitelist;
   }

   @Override
   protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
      boolean safeClass = whitelist.isSafeClass(desc.getName());
      if (!safeClass)
         throw CONTAINER.classNotInAllowList(desc.getName());

      return super.resolveClass(desc);
   }
}
