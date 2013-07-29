package org.infinispan.commons.marshall.jboss;

import org.infinispan.commons.marshall.Externalizer;
import org.jboss.marshalling.Creator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class JBossExternalizerAdapter implements org.jboss.marshalling.Externalizer {

   final Externalizer<Object> externalizer;

   @SuppressWarnings("unchecked")
   public JBossExternalizerAdapter(Externalizer<?> externalizer) {
      this.externalizer = (Externalizer<Object>) externalizer;
   }

   @Override
   public void writeExternal(Object subject, ObjectOutput output) throws IOException {
      externalizer.writeObject(output, subject);
   }

   @Override
   public Object createExternal(Class<?> subjectType, ObjectInput input, Creator defaultCreator) throws IOException, ClassNotFoundException {
      return externalizer.readObject(input);
   }

   @Override
   public void readExternal(Object subject, ObjectInput input) throws IOException, ClassNotFoundException {
      // No-op
   }

}
