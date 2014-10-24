package org.infinispan.server.core.security.simple;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.Principal;
import java.util.Collection;

import javax.security.auth.Subject;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.server.core.security.SubjectUserInfo;

/**
 * SimpleSubjectUserInfo.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@SerializeWith(SimpleSubjectUserInfo.Externalizer.class)
public class SimpleSubjectUserInfo implements SubjectUserInfo {
    final String userName;
    final Subject subject;

    public SimpleSubjectUserInfo(String userName, Subject subject) {
       this.userName = userName;
       this.subject = subject;
    }

    @Override
    public String getUserName() {
        return userName;
    }

   @Override
   public Collection<Principal> getPrincipals() {
      return subject.getPrincipals();
   }

   @Override
   public Subject getSubject() {
      return subject;
   }

   public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<SimpleSubjectUserInfo> {

      @Override
      public void writeObject(ObjectOutput output, SimpleSubjectUserInfo object) throws IOException {
         output.writeUTF(object.userName);
         output.writeObject(object.subject);
      }

      @Override
      public SimpleSubjectUserInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SimpleSubjectUserInfo(input.readUTF(), (Subject)input.readObject());
      }

   }
}
