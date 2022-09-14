package org.infinispan.security.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;

public class SubjectExternalizer implements AdvancedExternalizer<Subject> {

   @Override
   public Set<Class<? extends Subject>> getTypeClasses() {
      return Collections.singleton(Subject.class);
   }

   @Override
   public Integer getId() {
      return Ids.SUBJECT;
   }

   @Override
   public void writeObject(ObjectOutput output, Subject subject) throws IOException {
      Set<Principal> principals = subject.getPrincipals();
      output.writeInt(principals.size());
      for (Principal principal : principals) {
         output.writeUTF(principal.getName());
      }
   }

   @Override
   public Subject readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Subject subject = new Subject();
      int count = input.readInt();
      Set<Principal> p = subject.getPrincipals();
      for (int i = 0; i < count; i++) {
         p.add(new SimplePrincipal(input.readUTF()));
      }
      return subject;
   }

   public static class SimplePrincipal implements Principal {
      final String name;

      public SimplePrincipal(String name) {
         this.name = name;
      }

      @Override
      public String getName() {
         return name;
      }

      @Override
      public String toString() {
         return "SimplePrincipal{" +
               "name='" + name + '\'' +
               '}';
      }
   }
}
