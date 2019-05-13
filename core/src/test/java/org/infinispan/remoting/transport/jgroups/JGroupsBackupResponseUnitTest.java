package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.test.Exceptions.assertException;
import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongConsumer;

import org.infinispan.commons.CacheException;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.XSiteAsyncAckListener;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.xsite.XSiteBackup;
import org.testng.annotations.Test;

/**
 * Unit test for the methods of {@link BackupResponse}.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
@Test(groups = "unit", testName = "remoting.transport.jgroups.JGroupsBackupResponseUnitTest")
public class JGroupsBackupResponseUnitTest extends AbstractInfinispanTest {

   private final ControlledTimeService timeService = new ControlledTimeService();

   private static Map<XSiteBackup, CompletableFuture<ValidResponse>> createResponseMap(
         Collection<XSiteBackup> backups) {
      Map<XSiteBackup, CompletableFuture<ValidResponse>> responses = new HashMap<>(backups.size());
      for (XSiteBackup backup : backups) {
         responses.put(backup, new CompletableFuture<>());
      }
      return responses;
   }

   private static XSiteBackup createSyncBackup(String siteName, long timeoutMs) {
      return new XSiteBackup(siteName, true, timeoutMs);
   }

   private static XSiteBackup createAsyncBackup(String siteName) {
      return new XSiteBackup(siteName, false, 15000);
   }

   public void testNoWaitForAsyncWithMix() {
      List<XSiteBackup> backups = new ArrayList<>(2);
      backups.add(createSyncBackup("sync", 10000));
      backups.add(createAsyncBackup("async"));

      Map<XSiteBackup, CompletableFuture<ValidResponse>> responses = createResponseMap(backups);
      BackupResponse response = newBackupResponse(responses);

      Future<Void> waiting = waitBackupResponse(response);
      assertNotCompleted(waiting);

      //complete the sync request
      responses.get(backups.get(0)).complete(null);
      //it shouldn't wait for the async request
      assertCompleted(waiting);
   }

   public void testNoWaitForAsyncWith() {
      List<XSiteBackup> backups = Collections.singletonList(createAsyncBackup("async-only"));
      Map<XSiteBackup, CompletableFuture<ValidResponse>> responses = createResponseMap(backups);

      BackupResponse response = newBackupResponse(responses);
      Future<Void> waiting = waitBackupResponse(response);

      //we only have async. it should be completed
      assertCompleted(waiting);
   }

   public void testAsyncListener() {
      Listener listener = new Listener();
      long sendTimestamp = timeService.time();

      List<XSiteBackup> backups = new ArrayList<>(2);
      backups.add(createAsyncBackup("async-1"));
      backups.add(createAsyncBackup("async-2"));
      Map<XSiteBackup, CompletableFuture<ValidResponse>> responses = createResponseMap(backups);
      BackupResponse backupResponse = newBackupResponse(responses);

      backupResponse.notifyAsyncAck(listener);
      assertTrue(listener.queue.isEmpty());

      timeService.advance(10);
      responses.get(backups.get(0)).complete(null);
      assertListenerData(listener, sendTimestamp, "async-1", null);

      timeService.advance(10);
      CacheException exception = new CacheException("Test-Exception");
      responses.get(backups.get(1)).completeExceptionally(exception);
      assertListenerData(listener, sendTimestamp, "async-2", exception);

      assertTrue(listener.queue.isEmpty());
      assertEquals(TimeUnit.NANOSECONDS.toMillis(sendTimestamp), backupResponse.getSendTimeMillis());
   }

   public void testSyncListener() {
      Listener listener = new Listener();

      List<XSiteBackup> backups = new ArrayList<>(2);
      backups.add(createSyncBackup("sync-1", 10000));
      backups.add(createAsyncBackup("async-2"));
      Map<XSiteBackup, CompletableFuture<ValidResponse>> responses = createResponseMap(backups);
      BackupResponse backupResponse = newBackupResponse(responses);

      backupResponse.notifyFinish(listener);
      assertTrue(listener.queue.isEmpty());

      Future<Void> waiting = waitBackupResponse(backupResponse);

      timeService.advance(10);
      responses.get(backups.get(1)).complete(null);
      assertNotCompleted(waiting);
      assertTrue(listener.queue.isEmpty());

      timeService.advance(10);
      responses.get(backups.get(0)).complete(null);
      assertCompleted(waiting);
      assertListenerData(listener, 20, null, null);
      assertTrue(listener.queue.isEmpty());
   }

   public void testNoErrorsFromAsync() {
      //tests if JGroupsBackupResponse doesn't waitBackupResponse for the async request
      long timeoutMs = 10000;
      List<XSiteBackup> backups = new ArrayList<>(3);
      backups.add(createSyncBackup("sync-1", timeoutMs));
      backups.add(createSyncBackup("sync-2", 2 * timeoutMs));
      backups.add(createAsyncBackup("async"));

      Map<XSiteBackup, CompletableFuture<ValidResponse>> responses = createResponseMap(backups);
      BackupResponse response = newBackupResponse(responses);

      timeService.advance(timeoutMs + 1); //this will trigger a timeout for sync-1
      Future<Void> waiting = waitBackupResponse(response);
      assertNotCompleted(waiting);

      //complete the async request
      CacheException exception = new CacheException("Test-Exception");
      responses.get(backups.get(1)).complete(null);
      responses.get(backups.get(2)).completeExceptionally(exception);

      assertCompleted(waiting);

      assertEquals(1, response.getCommunicationErrors().size());
      assertEquals(1, response.getFailedBackups().size());
      assertTrue(response.getCommunicationErrors().contains("sync-1"));
      assertTrue(response.getFailedBackups().containsKey("sync-1"));
      assertException(org.infinispan.util.concurrent.TimeoutException.class, response.getFailedBackups().get("sync-1"));
   }

   public void testEmpty() {
      List<XSiteBackup> backups = new ArrayList<>(1);
      backups.add(createAsyncBackup("async"));

      BackupResponse response = newBackupResponse(createResponseMap(backups));
      assertTrue(response.isEmpty());

      backups.add(createSyncBackup("sync", 10000));
      response = newBackupResponse(createResponseMap(backups));
      assertFalse(response.isEmpty());
   }

   private void assertListenerData(Listener listener, long sendTimestamp, String siteName, Throwable throwable) {
      try {
         ListenerData data = listener.queue.poll(10, TimeUnit.SECONDS);
         assertNotNull("Failed to get event for site " + siteName, data);
         assertEquals(siteName, data.siteName);
         assertEquals(sendTimestamp, data.time);
         assertEquals(throwable, data.throwable);

      } catch (InterruptedException e) {
         fail("Interrupted while waiting for event for site " + siteName);
      }

   }

   private void assertNotCompleted(Future<Void> future) {
      expectException(TimeoutException.class, () -> future.get(1, TimeUnit.SECONDS));
   }

   private void assertCompleted(Future<Void> future) {
      try {
         future.get(1, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
         fail("Backup Response must be completed by now!");
      }
   }

   private Future<Void> waitBackupResponse(BackupResponse response) {
      return fork(response::waitForBackupToFinish);
   }

   private BackupResponse newBackupResponse(Map<XSiteBackup, CompletableFuture<ValidResponse>> responses) {
      return new JGroupsBackupResponse(responses, timeService);
   }

   private static class Listener implements XSiteAsyncAckListener, LongConsumer {

      private final BlockingDeque<ListenerData> queue = new LinkedBlockingDeque<>();

      @Override
      public void onAckReceived(long sendTimestamp, String siteName, Throwable throwable) {
         queue.add(new ListenerData(sendTimestamp, siteName, throwable));
      }

      @Override
      public void accept(long value) {
         //well, just lazy to create another listener data
         queue.add(new ListenerData(value, null, null));
      }
   }

   private static class ListenerData {
      private final long time;
      private final String siteName;
      private final Throwable throwable;

      private ListenerData(long time, String siteName, Throwable throwable) {
         this.time = time;
         this.siteName = siteName;
         this.throwable = throwable;
      }
   }

}
