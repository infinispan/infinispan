package org.infinispan.commons.tx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Random;

import javax.transaction.xa.Xid;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Pedro Ruivo
 * @since 9.1
 */
public class XidUnitTest {

   private static final Log log = LogFactory.getLog(XidUnitTest.class);

   @Test(expected = IllegalArgumentException.class)
   public void testInvalidGlobalTransaction() {
      long seed = System.currentTimeMillis();
      log.infof("[testInvalidGlobalTransaction] seed: %s", seed);
      Random random = new Random(seed);
      Xid xid = XidImpl.create(random.nextInt(), Util.EMPTY_BYTE_ARRAY, new byte[]{0});
      log.debugf("Invalid XID: %s", xid);
   }

   @Test(expected = IllegalArgumentException.class)
   public void testInvalidGlobalTransaction2() {
      long seed = System.currentTimeMillis();
      log.infof("[testInvalidGlobalTransaction2] seed: %s", seed);
      Random random = new Random(seed);
      byte[] globalTx = new byte[65]; //max is 64
      random.nextBytes(globalTx);
      Xid xid = XidImpl.create(random.nextInt(), globalTx, new byte[]{0});
      log.debugf("Invalid XID: %s", xid);
   }

   @Test(expected = IllegalArgumentException.class)
   public void testInvalidBranch() {
      long seed = System.currentTimeMillis();
      log.infof("[testInvalidBranch] seed: %s", seed);
      Random random = new Random(seed);
      Xid xid = XidImpl.create(random.nextInt(), new byte[]{0}, Util.EMPTY_BYTE_ARRAY);
      log.debugf("Invalid XID: %s", xid);
   }

   @Test(expected = IllegalArgumentException.class)
   public void testInvalidBranch2() {
      long seed = System.currentTimeMillis();
      log.infof("[testInvalidBranch2] seed: %s", seed);
      Random random = new Random(seed);
      byte[] branch = new byte[65]; //max is 64
      random.nextBytes(branch);
      Xid xid = XidImpl.create(random.nextInt(), new byte[]{0}, branch);
      log.debugf("Invalid XID: %s", xid);
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

      Assert.assertEquals(formatId, xid.getFormatId());
      Assert.assertArrayEquals(tx, xid.getGlobalTransactionId());
      Assert.assertArrayEquals(branch, xid.getBranchQualifier());

      Xid sameXid = XidImpl.create(formatId, tx, branch);
      log.debugf("same XID: %s", sameXid);
      Assert.assertEquals(xid, sameXid);
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
      Assert.assertEquals(formatId, xid.getFormatId());
      Assert.assertArrayEquals(tx, xid.getGlobalTransactionId());
      Assert.assertArrayEquals(branch, xid.getBranchQualifier());

      Xid sameXid = XidImpl.create(formatId, tx, branch);
      log.debugf("same XID: %s", sameXid);
      Assert.assertEquals(xid, sameXid);
   }

   @Test
   public void testMarshalling() throws IOException, ClassNotFoundException {
      long seed = System.currentTimeMillis();
      log.infof("[testMarshalling] seed: %s", seed);
      Random random = new Random(seed);

      int formatId = random.nextInt();
      byte[] tx = new byte[random.nextInt(64) + 1];
      byte[] branch = new byte[random.nextInt(64) + 1];
      random.nextBytes(tx);
      random.nextBytes(branch);
      XidImpl xid = XidImpl.create(formatId, tx, branch);

      log.debugf("XID: %s", xid);

      ByteArrayOutputStream bos = new ByteArrayOutputStream(256);
      ObjectOutput oo = new ObjectOutputStream(bos);
      XidImpl.writeTo(oo, xid);
      oo.flush();
      oo.close();
      bos.close();

      byte[] marshalled = bos.toByteArray();
      log.debugf("Size: %s", marshalled.length);

      ByteArrayInputStream bis = new ByteArrayInputStream(marshalled);
      ObjectInput oi = new ObjectInputStream(bis);
      XidImpl otherXid = XidImpl.readFrom(oi);
      oi.close();
      bis.close();

      log.debugf("other XID: %s", xid);

      Assert.assertEquals(xid, otherXid);
   }

   @Test
   public void testMarshallingMaxSize() throws IOException, ClassNotFoundException {
      long seed = System.currentTimeMillis();
      log.infof("[testMarshallingMaxSize] seed: %s", seed);
      Random random = new Random(seed);

      int formatId = random.nextInt();
      byte[] tx = new byte[Xid.MAXGTRIDSIZE];
      byte[] branch = new byte[Xid.MAXBQUALSIZE];
      random.nextBytes(tx);
      random.nextBytes(branch);
      XidImpl xid = XidImpl.create(formatId, tx, branch);

      log.debugf("XID: %s", xid);

      ByteArrayOutputStream bos = new ByteArrayOutputStream(256);
      ObjectOutput oo = new ObjectOutputStream(bos);
      XidImpl.writeTo(oo, xid);
      oo.flush();
      oo.close();
      bos.close();

      byte[] marshalled = bos.toByteArray();
      log.debugf("Size: %s", marshalled.length);

      ByteArrayInputStream bis = new ByteArrayInputStream(marshalled);
      ObjectInput oi = new ObjectInputStream(bis);
      XidImpl otherXid = XidImpl.readFrom(oi);
      oi.close();
      bis.close();

      log.debugf("other XID: %s", xid);

      Assert.assertEquals(xid, otherXid);
   }


}
