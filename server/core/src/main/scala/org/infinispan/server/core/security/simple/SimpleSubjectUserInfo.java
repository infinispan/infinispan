package org.infinispan.server.core.security.simple;

import java.security.Principal;
import java.util.Collection;

import javax.security.auth.Subject;

import org.infinispan.server.core.security.SubjectUserInfo;

/**
 * SimpleSubjectUserInfo.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SimpleSubjectUserInfo implements SubjectUserInfo {
    final String userName;
    final Subject subject;

    public SimpleSubjectUserInfo(String userName, Subject subject) {
       this.userName = userName;
       this.subject = subject;
    }

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
}
