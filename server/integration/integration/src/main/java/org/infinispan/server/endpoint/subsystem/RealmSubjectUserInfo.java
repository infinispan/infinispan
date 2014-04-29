package org.infinispan.server.endpoint.subsystem;

import java.security.Principal;
import java.util.Collection;

import javax.security.auth.Subject;

import org.infinispan.server.core.security.SubjectUserInfo;

/**
 * RealmSubjectUserInfo.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class RealmSubjectUserInfo implements SubjectUserInfo {
   private final String userName;
   private final Subject subject;

   RealmSubjectUserInfo(String userName, Subject subject) {
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

}
