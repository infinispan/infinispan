package org.infinispan.server.hotrod.test;

import static java.lang.String.format;
import static org.infinispan.commons.util.Util.printArray;
import static org.testng.AssertJUnit.assertArrayEquals;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.ControlByte;

/**
 * A test class that simulates a client's transaction.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class RemoteTransaction {

   private static final Log log = LogFactory.getLog(RemoteTransaction.class, Log.class);
   private static final int FORMAT = -1234;
   private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);
   private final HotRodClient client;
   private final XidImpl xid;
   private final Map<WrappedByteArray, ContextValue> txContext;

   private RemoteTransaction(HotRodClient client) {
      this.client = client;
      byte[] gtxAndBrandId = intToBytes(ID_GENERATOR.incrementAndGet());
      xid = XidImpl.create(FORMAT, gtxAndBrandId, gtxAndBrandId);
      txContext = new ConcurrentHashMap<>();
   }

   public static RemoteTransaction startTransaction(HotRodClient client) {
      return new RemoteTransaction(Objects.requireNonNull(client));
   }

   private static byte[] intToBytes(long val) {
      byte[] array = new byte[4];
      for (int i = 3; i > 0; i--) {
         array[i] = (byte) val;
         val >>>= 8;
      }
      array[0] = (byte) val;
      return array;
   }

   private static TxWrite transform(Map.Entry<WrappedByteArray, ContextValue> entry) {
      ContextValue contextValue = entry.getValue();
      if (contextValue.removed) {
         return TxWrite.remove(entry.getKey().getBytes(), ControlByte.REMOVE_OP.set(contextValue.control),
               contextValue.versionRead);
      } else {
         return TxWrite.put(entry.getKey().getBytes(), contextValue.value, contextValue.lifespan, contextValue.maxIdle,
               contextValue.control, contextValue.versionRead);
      }
   }

   private static boolean isModifiedFilter(Map.Entry<WrappedByteArray, ContextValue> entry) {
      return entry.getValue().modified;
   }

   private static ContextValue nonExisting() {
      return new ContextValue(0, ControlByte.NON_EXISTING.bit(), null);
   }

   private static ContextValue notRead(@SuppressWarnings("unused") WrappedByteArray ignoredKey) {
      return new ContextValue(0, ControlByte.NOT_READ.bit(), null);
   }

   public void getAndAssert(byte[] key, byte[] expectedValue) {
      final String message = format("[XID=%s] Wrong value for key %s", xid, printArray(key, true));
      if (expectedValue == null) {
         assertEquals(message, null, get(key));
      } else {
         assertArrayEquals(message, expectedValue, get(key));
      }
   }

   public void set(byte[] key, byte[] value) {
      set(key, value, 0, 0);
   }

   public void set(byte[] key, byte[] value, int lifespan, int maxIdle) {
      log.debugf("SET[%s] (%s, %s, %s, &s)", xid, printArray(key, true), printArray(value, true), lifespan, maxIdle);
      ContextValue contextValue = txContext.computeIfAbsent(new WrappedByteArray(key), RemoteTransaction::notRead);
      contextValue.set(value, lifespan, maxIdle);
   }

   public void remove(byte[] key) {
      ContextValue contextValue = txContext.computeIfAbsent(new WrappedByteArray(key), RemoteTransaction::notRead);
      contextValue.remove();
   }

   public void prepareAndAssert(int expectedXaCode) {
      final String message = format("[XID=%s] Wrong XA return code for prepare", xid);
      assertEquals(message, expectedXaCode, prepare().xaCode);
   }

   public void prepareAndAssert(HotRodClient another, int expectedXaCode) {
      final String message = format("[XID=%s] Wrong XA return code for prepare", xid);
      log.debugf("PREPARE[%s]", xid);
      assertEquals(message, expectedXaCode, ((TxResponse) another.prepareTx(xid, false, modifications())).xaCode);
   }

   public void commitAndAssert(int expectedXaCode) {
      final String message = format("[XID=%s] Wrong XA return code for commit", xid);
      assertEquals(message, expectedXaCode, commit().xaCode);
   }

   public void commitAndAssert(HotRodClient another, int expectedXaCode) {
      final String message = format("[XID=%s] Wrong XA return code for commit", xid);
      log.debugf("COMMIT[%s]", xid);
      assertEquals(message, expectedXaCode, ((TxResponse) another.commitTx(xid)).xaCode);
   }

   public void rollbackAndAssert(int expectedXaCode) {
      final String message = format("[XID=%s] Wrong XA return code for rollback", xid);
      assertEquals(message, expectedXaCode, rollback().xaCode);
   }

   public void rollbackAndAssert(HotRodClient another, int expectedXaCode) {
      final String message = format("[XID=%s] Wrong XA return code for rollback", xid);
      log.debugf("ROLLBACK[%s]", xid);
      assertEquals(message, expectedXaCode, ((TxResponse) another.rollbackTx(xid)).xaCode);
   }

   public XidImpl getXid() {
      return xid;
   }

   private TxResponse rollback() {
      log.debugf("ROLLBACK[%s]", xid);
      return (TxResponse) client.rollbackTx(xid);
   }

   private byte[] get(byte[] key) {
      log.debugf("GET[%s] (%s)", xid, printArray(key, true));
      ContextValue value = txContext.get(new WrappedByteArray(key));
      if (value != null) {
         return value.value;
      }
      return performGet(key).value;
   }

   private TxResponse prepare() {
      log.debugf("PREPARE[%s]", xid);
      return (TxResponse) client.prepareTx(xid, false, modifications());
   }

   private TxResponse commit() {
      log.debugf("COMMIT[%s]", xid);
      return (TxResponse) client.commitTx(xid);
   }

   private List<TxWrite> modifications() {
      return txContext.entrySet().stream().filter(RemoteTransaction::isModifiedFilter).map(RemoteTransaction::transform)
            .collect(Collectors.toList());
   }

   private ContextValue performGet(byte[] key) {
      TestGetWithMetadataResponse response = client.getWithMetadata(key, 0);
      ContextValue contextValue = response.data
            .map(bytes -> new ContextValue(response.dataVersion, (byte) 0, bytes))
            .orElseGet(RemoteTransaction::nonExisting);
      txContext.put(new WrappedByteArray(key), contextValue);
      return contextValue;
   }

   private static class ContextValue {
      private final long versionRead;
      private final byte control;
      private byte[] value;
      private int lifespan;
      private int maxIdle;
      private boolean modified;
      private boolean removed;

      private ContextValue(long versionRead, byte control, byte[] value) {
         this.versionRead = versionRead;
         this.control = control;
         this.value = value;
      }

      public void remove() {
         this.value = null;
         this.modified = true;
         this.removed = true;
      }

      public void set(byte[] value, int lifespan, int maxIdle) {
         this.value = value;
         this.lifespan = lifespan;
         this.maxIdle = maxIdle;
         this.modified = true;
         this.removed = false;
      }

   }
}
