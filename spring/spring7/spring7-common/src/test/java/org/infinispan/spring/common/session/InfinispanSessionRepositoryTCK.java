package org.infinispan.spring.common.session;

import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractInfinispanSessionRepository.InfinispanSession;
import org.infinispan.test.AbstractInfinispanTest;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.FlushMode;
import org.springframework.session.SaveMode;
import org.testng.annotations.Test;

@Test(groups = "functional")
public abstract class InfinispanSessionRepositoryTCK extends AbstractInfinispanTest {

   protected SpringCache springCache;

   protected AbstractInfinispanSessionRepository sessionRepository;
   protected MediaType mediaType;

   protected InfinispanSessionRepositoryTCK mediaType(MediaType mediaType) {
      this.mediaType = mediaType;
      return this;
   }

   @Override
   protected String parameters() {
      return mediaType.toString();
   }
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
      InfinispanSession session = sessionRepository.createSession();

      //then
      assertNotNull(session.getId());
      assertNotNull(session.getCreationTime());
      assertNull(sessionRepository.findById(session.getId()));
   }

   @Test
   public void testSavingNewSession() throws Exception {
      //given
      InfinispanSession session = sessionRepository.createSession();
      Instant lastAccessedTime = session.getLastAccessedTime();
      Duration maxInactiveInterval = session.getMaxInactiveInterval();

      //when
      sessionRepository.save(session);

      //then
      InfinispanSession savedSession = sessionRepository.findById(session.getId());
      assertNotNull(savedSession);
      assertTrue(savedSession.getAttributeNames().isEmpty());
      assertEquals(
            lastAccessedTime.truncatedTo(ChronoUnit.MILLIS),
            savedSession.getLastAccessedTime().truncatedTo(ChronoUnit.MILLIS));
      assertEquals(maxInactiveInterval, savedSession.getMaxInactiveInterval());
   }

   @Test
   public void testSavingUnchangedSession() throws Exception {
      //given
      InfinispanSession session = sessionRepository.createSession();
      Instant lastAccessedTime = session.getLastAccessedTime();
      Duration maxInactiveInterval = session.getMaxInactiveInterval();
      sessionRepository.save(session);

      //when
      session = sessionRepository.findById(session.getId());
      sessionRepository.save(session);

      //then
      InfinispanSession updatedSession = sessionRepository.findById(session.getId());
      assertTrue(updatedSession.getAttributeNames().isEmpty());
      assertEquals(
            lastAccessedTime.truncatedTo(ChronoUnit.MILLIS),
            updatedSession.getLastAccessedTime().truncatedTo(ChronoUnit.MILLIS));
      assertEquals(maxInactiveInterval, updatedSession.getMaxInactiveInterval());
   }

   @Test
   public void testSavingSessionWithUpdatedMaxInactiveInterval() throws Exception {
      //given
      InfinispanSession session = sessionRepository.createSession();
      sessionRepository.save(session);

      //when
      session = sessionRepository.findById(session.getId());
      Duration newMaxInactiveInterval = Duration.ofSeconds(123);
      session.setMaxInactiveInterval(newMaxInactiveInterval);
      sessionRepository.save(session);

      //then
      InfinispanSession savedSession = sessionRepository.findById(session.getId());
      assertEquals(newMaxInactiveInterval, savedSession.getMaxInactiveInterval());
   }

   @Test
   public void testSavingSessionWithUpdatedLastAccessedTime() throws Exception {
      //given
      InfinispanSession session = sessionRepository.createSession();
      sessionRepository.save(session);

      //when
      session = sessionRepository.findById(session.getId());
      Instant newLastAccessedTime = Instant.now().minusSeconds(300);
      session.setLastAccessedTime(newLastAccessedTime);
      sessionRepository.save(session);

      //then
      InfinispanSession savedSession = sessionRepository.findById(session.getId());
      assertEquals(
            newLastAccessedTime.truncatedTo(ChronoUnit.MILLIS),
            savedSession.getLastAccessedTime().truncatedTo(ChronoUnit.MILLIS));
   }

   @Test
   public void testSavingSessionWithUpdatedAttributes() throws Exception {
      //given
      InfinispanSession session = sessionRepository.createSession();
      session.setAttribute("changed", "oldValue");
      session.setAttribute("removed", "existingValue");
      sessionRepository.save(session);

      //when
      session = sessionRepository.findById(session.getId());
      session.setAttribute("added", "addedValue");
      session.setAttribute("changed", "newValue");
      session.removeAttribute("removed");
      sessionRepository.save(session);

      //then
      InfinispanSession savedSession = sessionRepository.findById(session.getId());
      assertEquals(savedSession.getAttribute("added"), "addedValue");
      assertEquals(savedSession.getAttribute("changed"), "newValue");
      assertNull(savedSession.getAttribute("removed"));
   }

   @Test
   public void testUpdatingTTLOnAccessingData() throws Exception {
      //when
      InfinispanSession session = sessionRepository.createSession();
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
      InfinispanSession session = sessionRepository.createSession();
      sessionRepository.save(session);
      sessionRepository.deleteById(session.getId());

      //then
      assertNull(sessionRepository.findById(session.getId()));
   }

   @Test(timeOut = 5000)
   public void testEvictingSession() throws Exception {
      //when
      InfinispanSession session = sessionRepository.createSession();
      session.setMaxInactiveInterval(Duration.ofSeconds(1));
      sessionRepository.save(session);

      //then
      while (sessionRepository.findById(session.getId()) != null) {
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
      InfinispanSession session = sessionRepository.createSession();

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
      InfinispanSession session = sessionRepository.createSession();
      // setLastAccessedTime will be called by Spring Session's SessionRepositoryRequestWrapper.getSession
      session.setLastAccessedTime(Instant.now());
      session.setAttribute("testAttribute", "oldValue");
      sessionRepository.save(session);

      //when
      InfinispanSession slowRequestSession = sessionRepository.findById(session.getId());
      slowRequestSession.setLastAccessedTime(Instant.now());
      slowRequestSession.getAttribute("testAttribute");
      InfinispanSession fastRequestSession = sessionRepository.findById(session.getId());
      fastRequestSession.setLastAccessedTime(Instant.now());
      fastRequestSession.setAttribute("testAttribute", "fastValue");
      sessionRepository.save(fastRequestSession);
      sessionRepository.save(slowRequestSession);

      //then
      assertNotSame(slowRequestSession, fastRequestSession);
      InfinispanSession updatedSession = sessionRepository.findById(session.getId());
      assertEquals("fastValue", updatedSession.getAttribute("testAttribute"));
   }

   @Test
   public void testConcurrentSessionAccessWithSaveModeOnGetAttribute() {
      //given
      sessionRepository.setSaveMode(SaveMode.ON_GET_ATTRIBUTE);
      InfinispanSession session = sessionRepository.createSession();
      // setLastAccessedTime will be called by Spring Session's SessionRepositoryRequestWrapper.getSession
      session.setLastAccessedTime(Instant.now());
      session.setAttribute("testAttribute", "oldValue");
      sessionRepository.save(session);

      //when
      InfinispanSession slowRequestSession = sessionRepository.findById(session.getId());
      slowRequestSession.setLastAccessedTime(Instant.now());
      slowRequestSession.getAttribute("testAttribute");
      InfinispanSession fastRequestSession = sessionRepository.findById(session.getId());
      fastRequestSession.setLastAccessedTime(Instant.now());
      fastRequestSession.setAttribute("testAttribute", "fastValue");
      sessionRepository.save(fastRequestSession);
      sessionRepository.save(slowRequestSession);

      //then
      assertNotSame(slowRequestSession, fastRequestSession);
      InfinispanSession updatedSession = sessionRepository.findById(session.getId());
      assertEquals("oldValue", updatedSession.getAttribute("testAttribute"));
   }

   @Test
   public void testConcurrentSessionAccessWithSaveModeAlways() {
      //given
      sessionRepository.setSaveMode(SaveMode.ALWAYS);
      InfinispanSession session = sessionRepository.createSession();
      // setLastAccessedTime will be called by Spring Session's SessionRepositoryRequestWrapper.getSession
      session.setLastAccessedTime(Instant.now());
      session.setAttribute("testAttribute", "oldValue");
      sessionRepository.save(session);

      //when
      InfinispanSession slowRequestSession = sessionRepository.findById(session.getId());
      slowRequestSession.setLastAccessedTime(Instant.now());
      InfinispanSession fastRequestSession = sessionRepository.findById(session.getId());
      fastRequestSession.setLastAccessedTime(Instant.now());
      fastRequestSession.setAttribute("testAttribute", "fastValue");
      sessionRepository.save(fastRequestSession);
      sessionRepository.save(slowRequestSession);

      //then
      assertNotSame(slowRequestSession, fastRequestSession);
      InfinispanSession updatedSession = sessionRepository.findById(session.getId());
      assertEquals("oldValue", updatedSession.getAttribute("testAttribute"));
   }

   @Test
   public void testFlushModeImmediate() throws Exception {
      //given
      sessionRepository.setFlushMode(FlushMode.IMMEDIATE);
      InfinispanSession existingSession = sessionRepository.createSession();

      //when
      InfinispanSession newSession = sessionRepository.createSession();
      existingSession.setAttribute("testAttribute", "testValue");

      //then
      InfinispanSession savedNewSession = sessionRepository.findById(newSession.getId());
      assertNotNull(savedNewSession);
      InfinispanSession savedExistingSession = sessionRepository.findById(existingSession.getId());
      assertEquals("testValue", savedExistingSession.getAttribute("testAttribute"));
   }

   protected void addEmptySessionWithPrincipal(AbstractInfinispanSessionRepository sessionRepository, String principalName) {
      InfinispanSession session = sessionRepository.createSession();
      session.setAttribute(PRINCIPAL_NAME_INDEX_NAME, principalName);
      sessionRepository.save(session);
   }
}
