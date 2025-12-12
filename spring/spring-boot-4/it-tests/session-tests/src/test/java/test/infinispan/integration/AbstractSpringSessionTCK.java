package test.infinispan.integration;

import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.session.SessionRepository;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@AutoConfigureTestRestTemplate
public class AbstractSpringSessionTCK {

   @Autowired
   private SessionRepository<AbstractInfinispanSessionRepository.InfinispanSession> sessionRepository;

   @Autowired
   protected InfinispanSessionListener httpSessionListener;

   @Autowired
   protected TestRestTemplate testRestTemplate;

   @LocalServerPort
   protected int port;

   @Test
   public void testCreatingSessionWhenUsingREST() {
      assertNull(httpSessionListener.getCreatedSession());
      assertNull(httpSessionListener.getDestroyedSession());

      HttpHeaders httpHeaders = testRestTemplate
            .withBasicAuth("user", "password")
            .headForHeaders(getTestURL());

      assertNotNull(httpSessionListener.getCreatedSession());
      String sessionId = getSessionId(httpHeaders);
      assertNotNull(sessionId);
      assertNotNull(sessionRepository.findById(sessionId));

      sessionRepository.deleteById(getSessionId(httpHeaders));
      assertNotNull(httpSessionListener.getDestroyedSession());
   }

   private String getTestURL() {
      return "http://localhost:" + port + "/test";
   }

   private String getSessionId(HttpHeaders httpHeaders) {
      List<String> sessionIdHeaders = httpHeaders.getValuesAsList("x-auth-token");
      return sessionIdHeaders.isEmpty() ? null : sessionIdHeaders.get(0);
   }
}
