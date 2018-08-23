package org.infinispan.integrationtests.spring.boot.session;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.session.MapSession;
import org.springframework.session.SessionRepository;

public class AbstractSpringSessionTCK {

   @Autowired
   private SessionRepository<MapSession> sessionRepository;

   @LocalServerPort
   private int port;

   @Test
   public void testCreatingSessionWhenUsingREST() throws Exception {
      //given
      TestRestTemplate restTemplate = new TestRestTemplate("user", "password");

      //when
      HttpHeaders httpHeaders = restTemplate.headForHeaders(getTestURL());

      //then
      Assert.assertNotNull(sessionRepository.getSession(getSessionId(httpHeaders)));
   }

   private String getTestURL() {
      return "http://localhost:" + port + "/test";
   }

   private String getSessionId(HttpHeaders httpHeaders) {
      return httpHeaders.getValuesAsList("x-auth-token").get(0);
   }
}
