package org.infinispan.profiling;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Random;

@Test(groups = "profiling", enabled = false, testName = "profiling.MemConsumptionTest")
public class MemConsumptionTest extends AbstractInfinispanTest {
   // adjust the next 4 values
   int numEntries = 1000000;
   int payloadSize = 60; // bytes
   int keySize = 10; // bytes
   PayloadType payloadType = PayloadType.STRINGS;

   enum PayloadType {
      STRINGS, BYTE_ARRAYS
   }

   int bytesPerCharacter = 2;

   Random r = new Random();

   public void testMemConsumption() throws IOException {
      int kBytesCached = (bytesPerCharacter * numEntries * (payloadSize + keySize)) / 1024;
      System.out.println("Bytes to be cached: " + NumberFormat.getIntegerInstance().format(kBytesCached) + " kb");

      Cache c = TestCacheManagerFactory.createCacheManager().getCache();
      for (int i = 0; i < numEntries; i++) {
         switch (payloadType) {
            case STRINGS:
               c.put(generateUniqueString(i, keySize), generateRandomString(payloadSize));
               break;
            case BYTE_ARRAYS:
               c.put(generateUniqueKey(i, keySize), generateBytePayload(payloadSize));
               break;
            default:
               throw new CacheException("Unknown payload type");
         }


         if (i % 1000 == 0) System.out.println("Added " + i + " entries");
      }

      System.out.println("Calling System.gc()");
      System.gc();  // clear any unnecessary objects

      TestingUtil.sleepThread(1000); // wait for gc

      // wait for manual test exit
      System.out.println("Cache populated; check mem usage using jconsole, etc.!");
      System.in.read();
   }

   private String generateUniqueString(int runNumber, int keySize) {
      // string size should be exactly equal to key size but also be unique.
      // start by creating a string from the run number
      StringBuilder sb = new StringBuilder();
      // append the run number
      sb.append(runNumber);
      for (int i = sb.length(); i < keySize; i++) sb.append("_");
      return sb.toString();
   }

   private byte[] generateUniqueKey(int runNumber, int keySize) {
      byte[] b = new byte[keySize];
      b[0] = (byte) (runNumber);
      b[1] = (byte) (runNumber >>> 8);
      b[2] = (byte) (runNumber >>> 16);
      b[3] = (byte) (runNumber >>> 24);

      for (int i = 4; i < keySize; i++) b[i] = 0;
      return b;
   }

   private byte[] generateBytePayload(int payloadSize) {
      byte[] b = new byte[payloadSize];
      Arrays.fill(b, (byte) 0);
      return b;
   }

   private String generateRandomString(int stringSize) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < stringSize; i++) {
         sb.append(r.nextInt(9)); // single digit
      }
      assert sb.length() == stringSize;
      return sb.toString();
   }
}
