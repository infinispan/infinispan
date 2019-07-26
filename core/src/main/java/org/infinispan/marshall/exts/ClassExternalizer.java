package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

public class ClassExternalizer implements AdvancedExternalizer<Class> {

   private final ClassLoader cl;

   public ClassExternalizer(ClassLoader cl) {
      this.cl = cl;
   }

   @Override
   public Set<Class<? extends Class>> getTypeClasses() {
      return Util.asSet(Class.class);
   }

   @Override
   public Integer getId() {
      return Ids.CLASS;
   }

   @Override
   public void writeObject(ObjectOutput out, Class o) throws IOException {
      out.writeUTF(o.getName());
   }

   @Override
   public Class readObject(ObjectInput in) throws IOException, ClassNotFoundException {
      return Class.forName(in.readUTF(), true, cl);
   }
}
