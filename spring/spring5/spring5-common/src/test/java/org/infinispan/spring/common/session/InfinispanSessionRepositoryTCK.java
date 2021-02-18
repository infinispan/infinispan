package org.infinispan.spring.common.session;

import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;
import org.testng.annotations.Test;

@Test(groups = "functional")
public abstract class InfinispanSessionRepositoryTCK extends AbstractInfinispanTest {

   protected SpringCache springCache;

   protected AbstractInfinispanSessionRepository sessionRepository;

   protected abstract SpringCache createSpringCache();

   protected abstract AbstractInfinispanSessionRepository createRepository(SpringCache springCache) throws Exception;

   protected void init() throws Exception {
      springCache = createSpringCache();
      sessionRepository = createRepository(springCache);
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testThrowingExceptionOnNullSpringCache() throws Exception {
      createRepository(null);
   }

   @Test
   public void testCreatingSession() throws Exception {
      //when
      MapSession session = sessionRepository.createSession();

      //then
      assertNotNull(session.getId());
      assertNotNull(session.getCreationTime());
      assertNull(sessionRepository.findById(session.getId()));
   }

   @Test
   public void testSavingSession() throws Exception {
      //when
      MapSession session = sessionRepository.createSession();
      sessionRepository.save(session);

      //then
      assertNotNull(sessionRepository.findById(session.getId()));
   }

   @Test
   public void testUpdatingTTLOnAccessingData() throws Exception {
      //when
      MapSession session = sessionRepository.createSession();
      long accessTimeBeforeSaving = session.getLastAccessedTime().toEpochMilli();

      sessionRepository.save(session);
      long accessTimeAfterSaving = session.getLastAccessedTime().toEpochMilli();

      long accessTimeAfterAccessing = sessionRepository.findById(session.getId()).getLastAccessedTime().toEpochMilli();

      long now = Instant.now().toEpochMilli();

      //then
      assertTrue(accessTimeBeforeSaving > 0);
      assertTrue(accessTimeBeforeSaving <= now);
      assertTrue(accessTimeAfterSaving > 0);
      assertTrue(accessTimeAfterSaving <= now);
      assertTrue(accessTimeAfterAccessing > 0);
      assertTrue(accessTimeAfterAccessing >= accessTimeAfterSaving);
   }

   @Test
   public void testDeletingSession() throws Exception {
      //when
      MapSession session = sessionRepository.createSession();
      sessionRepository.save(session);
      sessionRepository.deleteById(session.getId());

      //then
      assertNull(sessionRepository.findById(session.getId()));
   }

   @Test(timeOut = 5000)
   public void testEvictingSession() throws Exception {
      //when
      MapSession session = sessionRepository.createSession();
      session.setMaxInactiveInterval(Duration.ofSeconds(1));
      sessionRepository.save(session);

      //then
      while (sessionRepository.getSession(session.getId(), false) != null) {
         TimeUnit.MILLISECONDS.sleep(500);
      }
   }

   @Test
   public void testExtractingPrincipalWithWrongIndexName() throws Exception {
      //when
      int sizeWithWrongIndexName = ((FindByIndexNameSessionRepository) sessionRepository).findByIndexNameAndIndexValue("wrongIndexName", "").size();
      int sizeWithNullIndexName = ((FindByIndexNameSessionRepository) sessionRepository).findByIndexNameAndIndexValue(null, "").size();

      //then
      assertEquals(0, sizeWithNullIndexName);
      assertEquals(0, sizeWithWrongIndexName);
   }

   @Test
   public void testExtractingPrincipal() throws Exception {
      //given
      addEmptySessionWithPrincipal(sessionRepository, "test1");
      addEmptySessionWithPrincipal(sessionRepository, "test2");
      addEmptySessionWithPrincipal(sessionRepository, "test3");

      //when
      int numberOfTest1Users = ((FindByIndexNameSessionRepository) sessionRepository)
            .findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME, "test1").size();
      int numberOfNonExistingUsers = ((FindByIndexNameSessionRepository) sessionRepository)
            .findByIndexNameAndIndexValue(PRINCIPAL_NAME_INDEX_NAME, "notExisting").size();

      //then
      assertEquals(1, numberOfTest1Users);
      assertEquals(0, numberOfNonExistingUsers);
   }

   @Test
   public void testChangeSessionId() throws Exception {
      //given
      MapSession session = sessionRepository.createSession();

      //when
      String originalId = session.getId();
      sessionRepository.save(session);
      session.changeSessionId();
      String newId = session.getId();
      sessionRepository.save(session);

      //then
      assertNotNull(sessionRepository.findById(newId));
      assertNull(sessionRepository.findById(originalId));

      // Save again to test that the deletion doesn't fail when the session isn't in the repo
      sessionRepository.save(session );

      assertNotNull(sessionRepository.findById(newId));
      assertNull(sessionRepository.findById(originalId));
   }

   @Test
   public void testConcurrentSessionAccess() {
      //given
      MapSession session = sessionRepository.createSession();
      sessionRepository.save(session);

      //when
      MapSession concurrentRequestSession = sessionRepository.findById(session.getId());

      //then
      assertNotSame(session, concurrentRequestSession);

      // iterate over the attributes in one request and add an attribute in the other
      Map<String, Object> sessionAttrs = TestingUtil.extractField(session, "sessionAttrs");
      Iterator<Map.Entry<String, Object>> iterator = sessionAttrs.entrySet().iterator();

      concurrentRequestSession.setAttribute("foo", "bar");

      assertFalse(iterator.hasNext());
   }

   protected void addEmptySessionWithPrincipal(AbstractInfinispanSessionRepository sessionRepository, String principalName) {
      MapSession session = sessionRepository.createSession();
      session.setAttribute(PRINCIPAL_NAME_INDEX_NAME, principalName);
      sessionRepository.save(session);
   }
}
