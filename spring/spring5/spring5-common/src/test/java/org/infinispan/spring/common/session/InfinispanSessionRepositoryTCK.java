package org.infinispan.spring.common.session;

import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.test.AbstractInfinispanTest;
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
      assertTrue(session.getId() != null);
      assertTrue(session.getCreationTime() != 0);
      assertNull(sessionRepository.getSession(session.getId()));
   }

   @Test
   public void testSavingSession() throws Exception {
      //when
      MapSession session = sessionRepository.createSession();
      sessionRepository.save(session);

      //then
      assertNotNull(sessionRepository.getSession(session.getId()));
   }

   @Test
   public void testUpdatingTTLOnAccessingData() throws Exception {
      //when
      MapSession session = sessionRepository.createSession();
      long accessTimeBeforeSaving = session.getLastAccessedTime();

      sessionRepository.save(session);
      long accessTimeAfterSaving = session.getLastAccessedTime();

      long accessTimeAfterAccessing = sessionRepository.getSession(session.getId()).getLastAccessedTime();

      //then
      assertTrue(accessTimeBeforeSaving > 0);
      assertTrue(accessTimeBeforeSaving <= System.currentTimeMillis());
      assertTrue(accessTimeAfterSaving > 0);
      assertTrue(accessTimeAfterSaving <= System.currentTimeMillis());
      assertTrue(accessTimeAfterAccessing > 0);
      assertTrue(accessTimeAfterAccessing >= accessTimeAfterSaving);
   }

   @Test
   public void testDeletingSession() throws Exception {
      //when
      MapSession session = sessionRepository.createSession();
      sessionRepository.save(session);
      sessionRepository.delete(session.getId());

      //then
      assertNull(sessionRepository.getSession(session.getId()));
   }

   @Test(timeOut = 5000)
   public void testEvictingSession() throws Exception {
      //when
      MapSession session = sessionRepository.createSession();
      session.setMaxInactiveIntervalInSeconds(1);
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
      assertTrue(sizeWithNullIndexName == 0);
      assertTrue(sizeWithWrongIndexName == 0);
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
      assertTrue(numberOfTest1Users == 1);
      assertTrue(numberOfNonExistingUsers == 0);
   }

   protected void addEmptySessionWithPrincipal(AbstractInfinispanSessionRepository sessionRepository, String principalName) {
      MapSession session = sessionRepository.createSession();
      session.setAttribute(PRINCIPAL_NAME_INDEX_NAME, principalName);
      sessionRepository.save(session);
   }
}
