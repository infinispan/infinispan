package org.infinispan.security.impl;

import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

/**
 * @since 14.0
 **/
@ProtoAdapter(Subject.class)
@ProtoName("Subject")
public class SubjectAdapter {

   @ProtoFactory
   Subject create(String[] principals) {
      Subject subject = new Subject();
      Set<Principal> p = subject.getPrincipals();
      for (String principal : principals) {
         p.add(new SimplePrincipal(principal));
      }
      return subject;
   }

   @ProtoField(1)
   String[] getPrincipals(Subject subject) {
      return subject.getPrincipals().stream().map(Principal::getName).toArray(String[]::new);
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
