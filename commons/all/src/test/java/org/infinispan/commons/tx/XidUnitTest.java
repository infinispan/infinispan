package org.infinispan.commons.tx;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import javax.transaction.xa.Xid;

import org.infinispan.commons.GlobalContextInitializer;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.junit.jupiter.api.Test;

/**
 * @author Pedro Ruivo
 * @since 9.1
 */
public class XidUnitTest {

   private static final Log log = LogFactory.getLog(XidUnitTest.class);
   private final SerializationContext ctx;

   public XidUnitTest() {
      this.ctx = ProtobufUtil.newSerializationContext();
      GlobalContextInitializer.INSTANCE.register(ctx);
   }

   @Test
   public void testInvalidGlobalTransaction() {
      long seed = System.currentTimeMillis();
      log.infof("[testInvalidGlobalTransaction] seed: %s", seed);
      Random random = new Random(seed);
      assertThrows(IllegalArgumentException.class, () -> XidImpl.create(random.nextInt(), Util.EMPTY_BYTE_ARRAY, new byte[]{0}));
   }

   @Test
   public void testInvalidGlobalTransaction2() {
      long seed = System.currentTimeMillis();
      log.infof("[testInvalidGlobalTransaction2] seed: %s", seed);
      Random random = new Random(seed);
      byte[] globalTx = new byte[65]; //max is 64
      random.nextBytes(globalTx);
      assertThrows(IllegalArgumentException.class, () -> XidImpl.create(random.nextInt(), globalTx, new byte[]{0}));
   }

   @Test
   public void testInvalidBranch() {
      long seed = System.currentTimeMillis();
      log.infof("[testInvalidBranch] seed: %s", seed);
      Random random = new Random(seed);
      assertThrows(IllegalArgumentException.class, () -> XidImpl.create(random.nextInt(), new byte[]{0}, Util.EMPTY_BYTE_ARRAY));
   }

   @Test
   public void testInvalidBranch2() {
      long seed = System.currentTimeMillis();
      log.infof("[testInvalidBranch2] seed: %s", seed);
      Random random = new Random(seed);
      byte[] branch = new byte[65]; //max is 64
      random.nextBytes(branch);
      assertThrows(IllegalArgumentException.class, () -> XidImpl.create(random.nextInt(), new byte[]{0}, branch));
   }

   @Test
   public void testCorrectDataStored() {
      long seed = System.currentTimeMillis();
      log.infof("[testCorrectDataStored] seed: %s", seed);
      Random random = new Random(seed);

      int formatId = random.nextInt();
      byte[] tx = new byte[random.nextInt(64) + 1];
      byte[] branch = new byte[random.nextInt(64) + 1];
      random.nextBytes(tx);
      random.nextBytes(branch);
      Xid xid = XidImpl.create(formatId, tx, branch);
      log.debugf("XID: %s", xid);

      assertEquals(formatId, xid.getFormatId());
      assertArrayEquals(tx, xid.getGlobalTransactionId());
      assertArrayEquals(branch, xid.getBranchQualifier());

      Xid sameXid = XidImpl.create(formatId, tx, branch);
      log.debugf("same XID: %s", sameXid);
      assertEquals(xid, sameXid);
   }

   @Test
   public void testCorrectDataStoredMaxSize() {
      long seed = System.currentTimeMillis();
      log.infof("[testCorrectDataStoredMaxSize] seed: %s", seed);
      Random random = new Random(seed);
      int formatId = random.nextInt();

      byte[] tx = new byte[Xid.MAXGTRIDSIZE];
      byte[] branch = new byte[Xid.MAXBQUALSIZE];
      random.nextBytes(tx);
      random.nextBytes(branch);
      Xid xid = XidImpl.create(formatId, tx, branch);

      log.debugf("XID: %s", xid);
      assertEquals(formatId, xid.getFormatId());
      assertArrayEquals(tx, xid.getGlobalTransactionId());
      assertArrayEquals(branch, xid.getBranchQualifier());

      Xid sameXid = XidImpl.create(formatId, tx, branch);
      log.debugf("same XID: %s", sameXid);
      assertEquals(xid, sameXid);
   }

   @Test
   public void testMarshalling() throws IOException {
      long seed = System.currentTimeMillis();
      log.infof("[testMarshalling] seed: %s", seed);
      Random random = new Random(seed);
      assertIsCorrectlyMarshalled(random, random.nextInt(64) + 1, random.nextInt(64) + 1);
   }

   @Test
   public void testMarshallingMaxSize() throws IOException {
      long seed = System.currentTimeMillis();
      log.infof("[testMarshallingMaxSize] seed: %s", seed);
      Random random = new Random(seed);
      assertIsCorrectlyMarshalled(random, Xid.MAXGTRIDSIZE, Xid.MAXBQUALSIZE);
   }

   private void assertIsCorrectlyMarshalled(Random random, int txSize, int branchSize) throws IOException {
      int formatId = random.nextInt();
      byte[] tx = new byte[txSize];
      byte[] branch = new byte[branchSize];
      random.nextBytes(tx);
      random.nextBytes(branch);
      XidImpl xid = XidImpl.create(formatId, tx, branch);
      log.debugf("XID: %s", xid);

      ByteArrayOutputStream bos = new ByteArrayOutputStream(256);
      ProtobufUtil.toWrappedStream(ctx, bos, xid);
      bos.close();

      byte[] marshalled = bos.toByteArray();
      log.debugf("Size: %s", marshalled.length);

      try (ByteArrayInputStream bis = new ByteArrayInputStream(marshalled)) {
         XidImpl otherXid = ProtobufUtil.fromWrappedStream(ctx, bis);
         log.debugf("other XID: %s", xid);
         assertEquals(xid, otherXid);
      }
   }
}
