package org.infinispan.integrationtests.spring.boot.session.configuration;

import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

public class InfinispanSessionListener implements HttpSessionListener {

   private HttpSession createdSession;
   private HttpSession destroyedSession;

   @Override
   public void sessionCreated(HttpSessionEvent httpSessionEvent) {
      createdSession = httpSessionEvent.getSession();
   }

   @Override
   public void sessionDestroyed(HttpSessionEvent httpSessionEvent) {
      destroyedSession = httpSessionEvent.getSession();
   }

   public HttpSession getCreatedSession() {
      return createdSession;
   }

   public HttpSession getDestroyedSession() {
      return destroyedSession;
   }
}
