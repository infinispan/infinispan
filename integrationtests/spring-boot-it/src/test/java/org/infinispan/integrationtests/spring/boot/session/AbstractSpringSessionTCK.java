package org.infinispan.integrationtests.spring.boot.session;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.infinispan.integrationtests.spring.boot.session.configuration.InfinispanSessionListener;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.session.MapSession;
import org.springframework.session.SessionRepository;

public class AbstractSpringSessionTCK {

   @Autowired
   private SessionRepository<MapSession> sessionRepository;

   @Autowired
   protected InfinispanSessionListener httpSessionListener;

   @LocalServerPort
   private int port;

   @Test
   public void testCreatingSessionWhenUsingREST() {
      assertNull(httpSessionListener.getCreatedSession());
      assertNull(httpSessionListener.getDestroyedSession());

      TestRestTemplate restTemplate = new TestRestTemplate("user", "password");
      HttpHeaders httpHeaders = restTemplate.headForHeaders(getTestURL());

      assertNotNull(sessionRepository.findById(getSessionId(httpHeaders)));
      assertNotNull(httpSessionListener.getCreatedSession());

      sessionRepository.deleteById(getSessionId(httpHeaders));

      assertNotNull(httpSessionListener.getDestroyedSession());
   }

   private String getTestURL() {
      return "http://localhost:" + port + "/test";
   }

   private String getSessionId(HttpHeaders httpHeaders) {
      return httpHeaders.getValuesAsList("x-auth-token").get(0);
   }
}
