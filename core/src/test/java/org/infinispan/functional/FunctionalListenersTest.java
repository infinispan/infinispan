package org.infinispan.functional;

import static org.infinispan.functional.FunctionalTestUtils.rw;
import static org.infinispan.functional.FunctionalTestUtils.supplyIntKey;
import static org.infinispan.functional.FunctionalTestUtils.wo;
import static org.infinispan.marshall.core.MarshallableFunctions.removeConsumer;
import static org.infinispan.marshall.core.MarshallableFunctions.removeReturnPrevOrNull;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueConsumer;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueReturnPrevOrNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.functional.Listeners.ReadWriteListeners.ReadWriteListener;
import org.infinispan.functional.Listeners.WriteListeners.WriteListener;
import org.infinispan.functional.TestFunctionalInterfaces.SetConstantOnReadWrite;
import org.infinispan.functional.TestFunctionalInterfaces.SetConstantOnWriteOnly;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
import org.infinispan.test.TestDataSCI;
import org.testng.annotations.Test;

/**
 * Test suite for verifying functional map listener functionality.
 */
@Test(groups = "functional", testName = "functional.FunctionalListenersTest")
public class FunctionalListenersTest extends AbstractFunctionalTest {

   public FunctionalListenersTest() {
      this.serializationContextInitializer = new FunctionalListenersSCIImpl();
   }

   public void testSimpleLambdaReadWriteListeners() throws Exception {
      doLambdaReadWriteListeners(supplyIntKey(), wo(fmapS1), rw(fmapS2), true);
   }


   public void testLocalLambdaReadWriteListeners() throws Exception {
      doLambdaReadWriteListeners(supplyIntKey(), wo(fmapL1), rw(fmapL2), true);
   }

   public void testReplLambdaReadWriteListeners() throws Exception {
      doLambdaReadWriteListeners(supplyKeyForCache(0, REPL), wo(fmapR1), rw(fmapR2), true);
      doLambdaReadWriteListeners(supplyKeyForCache(1, REPL), wo(fmapR1), rw(fmapR2), true);
   }

   @Test
   public void testDistLambdaReadWriteListeners() throws Exception {
      doLambdaReadWriteListeners(supplyKeyForCache(0, DIST), wo(fmapD1), rw(fmapD2), false);
      doLambdaReadWriteListeners(supplyKeyForCache(1, DIST), wo(fmapD1), rw(fmapD2), true);
   }

   private <K> void doLambdaReadWriteListeners(Supplier<K> keySupplier,
         WriteOnlyMap<K, String> woMap, ReadWriteMap<K, String> rwMap, boolean isOwner) throws Exception {
      K key1 = keySupplier.get();
      List<CountDownLatch> latches = new ArrayList<>();
      latches.addAll(Arrays.asList(new CountDownLatch(1), new CountDownLatch(1), new CountDownLatch(1)));
      AutoCloseable onCreate = rwMap.listeners().onCreate(created -> {
         assertEquals("created", created.get());
         latches.get(0).countDown();
      });
      AutoCloseable onModify = rwMap.listeners().onModify((before, after) -> {
         assertEquals("created", before.get());
         assertEquals("modified", after.get());
         latches.get(1).countDown();
      });
      AutoCloseable onRemove = rwMap.listeners().onRemove(removed -> {
         assertEquals("modified", removed.get());
         latches.get(2).countDown();
      });

      awaitNoEvent(woMap.eval(key1, "created", setValueConsumer()), latches.get(0));
      awaitNoEvent(woMap.eval(key1, "modified", setValueConsumer()), latches.get(1));
      awaitNoEvent(woMap.eval(key1, removeConsumer()), latches.get(2));

      K key2 = keySupplier.get();
      awaitEventIfOwner(isOwner, rwMap.eval(key2, "created", setValueReturnPrevOrNull()), latches.get(0));
      awaitEventIfOwner(isOwner, rwMap.eval(key2, "modified", setValueReturnPrevOrNull()), latches.get(1));
      awaitEventIfOwner(isOwner, rwMap.eval(key2, removeReturnPrevOrNull()), latches.get(2));

      launderLatches(latches, 3);

      K key3 = keySupplier.get();
      awaitEventIfOwner(isOwner, rwMap.eval(key3, new SetConstantOnReadWrite<>("created")), latches.get(0));
      awaitEventIfOwner(isOwner, rwMap.eval(key3, new SetConstantOnReadWrite<>("modified")), latches.get(1));
      awaitEventIfOwner(isOwner, rwMap.eval(key3, removeReturnPrevOrNull()), latches.get(2));

      // TODO: Test other evals....

      onCreate.close();
      onModify.close();
      onRemove.close();

      launderLatches(latches, 3);

      K key4 = keySupplier.get();
      awaitNoEvent(woMap.eval(key4, "tres", setValueConsumer()), latches.get(0));
      awaitNoEvent(woMap.eval(key4, "three", setValueConsumer()), latches.get(1));
      awaitNoEvent(woMap.eval(key4, removeConsumer()), latches.get(2));

      K key5 = keySupplier.get();
      awaitNoEvent(rwMap.eval(key5, "cuatro", setValueReturnPrevOrNull()), latches.get(0));
      awaitNoEvent(rwMap.eval(key5, "four", setValueReturnPrevOrNull()), latches.get(1));
      awaitNoEvent(rwMap.eval(key5, removeReturnPrevOrNull()), latches.get(2));
   }

   public void testSimpleLambdaWriteListeners() throws Exception {
      doLambdaWriteListeners(supplyIntKey(), wo(fmapS1), true);
   }

   public void testLocalLambdaWriteListeners() throws Exception {
      doLambdaWriteListeners(supplyIntKey(), wo(fmapL1), true);
   }

   @Test
   public void testReplLambdaWriteListeners() throws Exception {
      doLambdaWriteListeners(supplyKeyForCache(0, REPL), wo(fmapR1), true);
      doLambdaWriteListeners(supplyKeyForCache(1, REPL), wo(fmapR2), true);
   }

   @Test
   public void testDistLambdaWriteListeners() throws Exception {
      doLambdaWriteListeners(supplyKeyForCache(0, DIST), wo(fmapD1), true);
      doLambdaWriteListeners(supplyKeyForCache(0, DIST), wo(fmapD2), false);
   }

   private <K> void doLambdaWriteListeners(Supplier<K> keySupplier,
         WriteOnlyMap<K, String> wo, boolean isOwner) throws Exception {
      K key1 = keySupplier.get(), key2 = keySupplier.get();
      List<CountDownLatch> latches = launderLatches(new ArrayList<>(), 1);
      AutoCloseable onWrite = wo.listeners().onWrite(read -> {
         assertEquals("write", read.get());
         latches.get(0).countDown();
      });

      awaitEventIfOwnerAndLaunderLatch(isOwner, wo.eval(key1, new SetConstantOnWriteOnly("write")), latches);
      awaitEventIfOwnerAndLaunderLatch(isOwner, wo.eval(key1, new SetConstantOnWriteOnly("write")), latches);
      onWrite.close();
      awaitNoEvent(wo.eval(key2, new SetConstantOnWriteOnly("write")), latches.get(0));
      awaitNoEvent(wo.eval(key2, new SetConstantOnWriteOnly("write")), latches.get(0));

      // TODO: Test other eval methods :)

      AutoCloseable onWriteRemove = wo.listeners().onWrite(read -> {
         assertFalse(read.find().isPresent());
         latches.get(0).countDown();
      });

      awaitEventIfOwnerAndLaunderLatch(isOwner, wo.eval(key1, removeConsumer()), latches);
      onWriteRemove.close();
      awaitNoEvent(wo.eval(key2, removeConsumer()), latches.get(0));
   }

//   @Test
//   public void testObjectReadWriteListeners() throws Exception {
//      TrackingReadWriteListener<Integer, String> listener = new TrackingReadWriteListener<>();
//      AutoCloseable closeable = readWriteMap.listeners().add(listener);
//
//      awaitNoEvent(writeOnlyMap.eval(1, writeView -> writeView.set("created")), listener.latch);
//      awaitNoEvent(writeOnlyMap.eval(1, writeView -> writeView.set("modified")), listener.latch);
//      awaitNoEvent(writeOnlyMap.eval(1, WriteEntryView::remove), listener.latch);
//
//      awaitEvent(readWriteMap.eval(2, rwView -> rwView.set("created")), listener.latch);
//      awaitEvent(readWriteMap.eval(2, rwView -> rwView.set("modified")), listener.latch);
//      awaitEvent(readWriteMap.eval(2, ReadWriteEntryView::remove), listener.latch);
//
//      closeable.close();
//      awaitNoEvent(writeOnlyMap.eval(3, writeView -> writeView.set("tres")), listener.latch);
//      awaitNoEvent(writeOnlyMap.eval(3, writeView -> writeView.set("three")), listener.latch);
//      awaitNoEvent(writeOnlyMap.eval(3, WriteEntryView::remove), listener.latch);
//
//      awaitNoEvent(readWriteMap.eval(4, rwView -> rwView.set("cuatro")), listener.latch);
//      awaitNoEvent(readWriteMap.eval(4, rwView -> rwView.set("four")), listener.latch);
//      awaitNoEvent(readWriteMap.eval(4, ReadWriteEntryView::remove), listener.latch);
//   }
//
//   @Test
//   public void testObjectWriteListeners() throws Exception {
//      TrackingWriteListener<Integer, String> writeListener = new TrackingWriteListener<>();
//      AutoCloseable writeListenerCloseable = writeOnlyMap.listeners().add(writeListener);
//
//      awaitEvent(writeOnlyMap.eval(1, writeView -> writeView.set("write")), writeListener.latch);
//      awaitEvent(writeOnlyMap.eval(1, writeView -> writeView.set("write")), writeListener.latch);
//      writeListenerCloseable.close();
//      awaitNoEvent(writeOnlyMap.eval(2, writeView -> writeView.set("write")), writeListener.latch);
//      awaitNoEvent(writeOnlyMap.eval(2, writeView -> writeView.set("write")), writeListener.latch);
//
//      TrackingRemoveOnWriteListener<Integer, String> writeRemoveListener = new TrackingRemoveOnWriteListener<>();
//      AutoCloseable writeRemoveListenerCloseable = writeOnlyMap.listeners().add(writeRemoveListener);
//
//      awaitEvent(writeOnlyMap.eval(1, WriteEntryView::remove), writeRemoveListener.latch);
//      writeRemoveListenerCloseable.close();
//      awaitNoEvent(writeOnlyMap.eval(2, WriteEntryView::remove), writeRemoveListener.latch);
//   }

   private static List<CountDownLatch> launderLatches(List<CountDownLatch> latches, int numLatches) {
      latches.clear();
      for (int i = 0; i < numLatches; i++)
         latches.add(new CountDownLatch(1));

      return latches;
   }

   public static <T> T awaitEvent(CompletableFuture<T> cf, CountDownLatch eventLatch) {
      try {
         T t = cf.get();
         assertTrue(eventLatch.await(500, TimeUnit.MILLISECONDS));
         return t;
      } catch (InterruptedException | ExecutionException e) {
         throw new Error(e);
      }
   }

   public static <T> T awaitNoEvent(CompletableFuture<T> cf, CountDownLatch eventLatch) {
      try {
         T t = cf.get();
         assertFalse(eventLatch.await(50, TimeUnit.MILLISECONDS));
         return t;
      } catch (InterruptedException | ExecutionException e) {
         throw new Error(e);
      }
   }

   public static <T> T awaitEventIfOwner(boolean isOwner, CompletableFuture<T> cf, CountDownLatch eventLatch) {
      return isOwner ? awaitEvent(cf, eventLatch) : awaitNoEvent(cf, eventLatch);
   }

   public static <T> T awaitEventAndLaunderLatch(CompletableFuture<T> cf, List<CountDownLatch> latches) {
      T t = awaitEvent(cf, latches.get(0));
      launderLatches(latches, 1);
      return t;
   }

   public static <T> T awaitEventIfOwnerAndLaunderLatch(boolean isOwner,
         CompletableFuture<T> cf, List<CountDownLatch> latches) {
      if (isOwner) {
         T t = awaitEvent(cf, latches.get(0));
         launderLatches(latches, 1);
         return t;
      }
      return awaitNoEvent(cf, latches.get(0));
   }

   private static final class TrackingReadWriteListener<K, V> implements ReadWriteListener<K, V> {
      CountDownLatch latch = new CountDownLatch(1);

      @Override
      public void onCreate(ReadEntryView<K, V> created) {
         assertEquals("created", created.get());
         latchCountAndLaunder();
      }

      @Override
      public void onModify(ReadEntryView<K, V> before, ReadEntryView<K, V> after) {
         assertEquals("created", before.get());
         assertEquals("modified", after.get());
         latchCountAndLaunder();
      }

      @Override
      public void onRemove(ReadEntryView<K, V> removed) {
         assertEquals("modified", removed.get());
         latchCountAndLaunder();
      }

      private void latchCountAndLaunder() {
         latch.countDown();
         latch = new CountDownLatch(1);
      }
   }

   public static final class TrackingWriteListener<K, V> implements WriteListener<K, V> {
      CountDownLatch latch = new CountDownLatch(1);

      @Override
      public void onWrite(ReadEntryView<K, V> write) {
         assertEquals("write", write.get());
         latchCountAndLaunder();
      }

      private void latchCountAndLaunder() {
         latch.countDown();
         latch = new CountDownLatch(1);
      }
   }

   public static final class TrackingRemoveOnWriteListener<K, V> implements WriteListener<K, V> {
      CountDownLatch latch = new CountDownLatch(1);

      @Override
      public void onWrite(ReadEntryView<K, V> write) {
         assertFalse(write.find().isPresent());
         latchCountAndLaunder();
      }

      private void latchCountAndLaunder() {
         latch.countDown();
         latch = new CountDownLatch(1);
      }
   }

   @ProtoSchema(
         dependsOn = TestDataSCI.class,
         includeClasses = {
               TestFunctionalInterfaces.SetConstantOnReadWrite.class,
               TestFunctionalInterfaces.SetConstantOnWriteOnly.class
         },
         schemaFileName = "test.core.functional.listeners.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.functional.listeners",
         service = false,
         syntax = ProtoSyntax.PROTO3
   )
   public interface FunctionalListenersSCI extends SerializationContextInitializer {
   }
}
