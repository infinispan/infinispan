package org.infinispan.distribution;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.infinispan.commons.util.Util.padString;
import static org.infinispan.commons.util.Util.prettyPrintTime;
import static org.infinispan.profiling.testinternals.Generator.*;

/**
 * This test benchmarks different hash functions.
 */
@Test (groups = "manual", testName = "distribution.HashFunctionComparisonTest")
public class HashFunctionComparisonTest {

   private static final int MAX_STRING_SIZE = 16;
   private static final int MAX_BYTE_ARRAY_SIZE = 16;
   private static final int NUM_KEYS_PER_TYPE = 1000 * 100;
   private static final int MODULUS_BASE = 1024;
   private static final NumberFormat nf = NumberFormat.getInstance();

   private static List<HashFunction> getHashFunctions() {
      List<HashFunction> functions = new LinkedList<HashFunction>();
      functions.add(new MurmurHash2());
      functions.add(new MurmurHash2Compat());
      functions.add(new MurmurHash3());
      functions.add(new SuperFastHash());
      return functions;
   }


   public void doTest() {
      for (int i : Arrays.asList(10, 50, 100, 500, 1000)) {
         System.out.printf("-----------------  Testing with %s nodes -----------------%n", i);
         addressDistribution(i);
      }
   }

   /**
    * Tests how well JGroupsAddresses are distributed on a hash wheel.
    */
   private void addressDistribution(int numAddresses) {
      int hashSpace = 10240;

      Collection<HashFunction> functions = getHashFunctions();

      System.out.printf("%s %s %s %s %s %n%n", padString("Function", 25), padString("Greatest dist", 15), padString("Smallest dist", 15), padString("Mean dist", 15), padString("Positions", 15));

      for (HashFunction f : functions) {

         List<Address> addresses = new LinkedList<Address>();
         for (int i=0; i<numAddresses; i++) addresses.add(generateAddress());

         SortedMap<Integer, Address> positions = new TreeMap<Integer, Address>();
         for (Address a : addresses) positions.put(f.hash(a.hashCode()) % hashSpace, a);

         System.out.printf("%s %s %s %s %s %n%n",
                 padString(f.functionName(), 25),
                 padString(greatestDist(positions, hashSpace), 15),
                 padString(smallestDist(positions, hashSpace), 15),
                 padString(meanDist(positions, hashSpace), 15),
//                 positions);
         "-");
      }

      System.out.printf("%s %s %s %s %s %n%n",
              padString("Perfectly Balanced", 25),
              padString("-", 15),
              padString("-", 15),
              padString(Integer.toString(hashSpace / numAddresses), 15),
              "-");

   }

   private String greatestDist(SortedMap<Integer, Address> pos, int hashSpace) {
      // calc distances between entries 0 and n - 1 first.
      int largest = 0;
      int lastPos = lastEntry(pos).getKey();
      int firstPos = -1;
      for (int currentPos: pos.keySet()) {
         if (firstPos == -1) firstPos = currentPos;
         largest = Math.max(largest, Math.abs(currentPos - lastPos));
         lastPos = currentPos;
      }

      return String.valueOf(largest);
   }

   private String smallestDist(SortedMap<Integer, Address> pos, int hashSpace) {
      // calc distances between entries 0 and n - 1 first.
      int smallest = Integer.MAX_VALUE;
      int lastPos = lastEntry(pos).getKey();
      int firstPos = -1;
      for (int currentPos: pos.keySet()) {
         if (firstPos == -1) firstPos = currentPos;
         smallest = Math.min(smallest, Math.abs(currentPos - lastPos));
         lastPos = currentPos;
      }
      return String.valueOf(smallest);
   }

   private String meanDist(SortedMap<Integer, Address> pos, int hashSpace) {
      // calc distances between entries 0 and n - 1 first.
      int totalDist = 0;
      int lastPos = lastEntry(pos).getKey();
      int firstPos = -1;
      for (int currentPos: pos.keySet()) {
         if (firstPos == -1) firstPos = currentPos;         
         totalDist += Math.abs(currentPos - lastPos);
         lastPos = currentPos;
      }

      return String.valueOf(totalDist / pos.size());
   }

   private Map.Entry<Integer, Address> lastEntry(SortedMap<Integer, Address> m) {
      Map.Entry<Integer, Address> last = null;
      for (Map.Entry<Integer, Address> e: m.entrySet()) last = e;
      return last;
   }


   public void testHashFunctions() {
      Collection<HashFunction> functions = getHashFunctions();

      Set<Object> objectKeys = new HashSet<Object>(NUM_KEYS_PER_TYPE);
      Set<String> stringKeys = new HashSet<String>(NUM_KEYS_PER_TYPE);
      Set<byte[]> byteArrayKeys = new HashSet<byte[]>(NUM_KEYS_PER_TYPE);

      // generate keys
      for (int i = 0; i < NUM_KEYS_PER_TYPE; i++) {
         String s = getRandomString(MAX_STRING_SIZE);
         objectKeys.add(s);
         stringKeys.add(s);
         byteArrayKeys.add(getRandomByteArray(MAX_BYTE_ARRAY_SIZE));
      }

      perform(functions, objectKeys, stringKeys, byteArrayKeys, false);
      perform(functions, objectKeys, stringKeys, byteArrayKeys, true);
   }

   private void captureStats(int hash, DescriptiveStatistics stats) {
      // comment this impl out if measuring raw performance
      stats.addValue(hash % MODULUS_BASE);
   }

   private void perform(Collection<HashFunction> functions, Set<Object> objectKeys, Set<String> stringKeys, Set<byte[]> byteArrayKeys, boolean warmup) {

      if (!warmup)
         System.out.printf("%s %s %s %s%n", padString("Function Impl", 25), padString("String keys", 18), padString("Byte array keys", 18), padString("Object keys", 18));

      for (HashFunction f : functions) {
         long oRes = 0, sRes = 0, bRes = 0;
         DescriptiveStatistics oStats = new DescriptiveStatistics();
         DescriptiveStatistics sStats = new DescriptiveStatistics();
         DescriptiveStatistics bStats = new DescriptiveStatistics();

         long st = System.currentTimeMillis();
         for (Object o : objectKeys) captureStats(f.hash(o.hashCode()), oStats);
         oRes = System.currentTimeMillis() - st;

         st = System.currentTimeMillis();
         for (String s : stringKeys) captureStats(f.hash(s), sStats);
         sRes = System.currentTimeMillis() - st;

         st = System.currentTimeMillis();
         for (byte[] b : byteArrayKeys) captureStats(f.hash(b), bStats);
         bRes = System.currentTimeMillis() - st;

         if (!warmup) {
            System.out.printf("%s %s %s %s%n",
                    padString(f.functionName(), 25),
                    padString(prettyPrintTime(sRes), 18),
                    padString(prettyPrintTime(bRes), 18),
                    padString(prettyPrintTime(oRes), 18)
            );
            System.out.printf("%s %s %s %s%n",
                    padString("  mean", 25),
                    padDouble(sStats.getMean()),
                    padDouble(bStats.getMean()),
                    padDouble(oStats.getMean())
            );
            System.out.printf("%s %s %s %s%n",
                    padString("  median", 25),
                    padDouble(sStats.getPercentile(50.0)),
                    padDouble(bStats.getPercentile(50.0)),
                    padDouble(oStats.getPercentile(50.0))
            );
            System.out.printf("%s %s %s %s%n",
                    padString("  deviation", 25),
                    padDouble(sStats.getStandardDeviation()),
                    padDouble(bStats.getStandardDeviation()),
                    padDouble(oStats.getStandardDeviation())
            );

            System.out.printf("%s %s %s %s%n",
                    padString("  variance", 25),
                    padDouble(sStats.getVariance()),
                    padDouble(bStats.getVariance()),
                    padDouble(oStats.getVariance())
            );
         }
      }
   }

   private String padDouble(double d) {
      return padString(nf.format(d), 18);
   }
}

abstract class HashFunction {
   abstract int hash(byte[] payload);

   public int hash(String payload) {
      return hash(payload.getBytes(Charset.forName("UTF-8")));
   }

   public int hash(int hashcode) {
      byte[] b = new byte[4];
      b[0] = (byte) hashcode;
      b[1] = (byte) (hashcode >> 8);
      b[2] = (byte) (hashcode >> 16);
      b[3] = (byte) (hashcode >> 24);
      return hash(b);
   }

   abstract String functionName();
}

class SuperFastHash extends HashFunction {

   @Override
   public int hash(byte[] data) {
      if (data == null || data.length == 0) return 0;
      int len = data.length;
      int hash = len;
      int rem = len & 3;
      len >>= 2;
      int tmp;
      int offset = 0;
      /* Main loop */
      for (; len > 0; len--) {
         hash += get16bits(data, offset);
         tmp = (get16bits(data, offset + 2) << 11) ^ hash;
         hash = (hash << 16) ^ tmp;
         offset += 4;
         hash += hash >> 11;
      }

      /* Handle end cases */
      switch (rem) {
         case 3:
            hash += get16bits(data, offset);
            hash ^= hash << 16;
            hash ^= data[2] << 18;
            hash += hash >> 11;
            break;
         case 2:
            hash += get16bits(data, offset);
            hash ^= hash << 11;
            hash += hash >> 17;
            break;
         case 1:
            hash += data[0];
            hash ^= hash << 10;
            hash += hash >> 1;
      }

      /* Force "avalanching" of final 127 bits */
      hash ^= hash << 3;
      hash += hash >> 5;
      hash ^= hash << 4;
      hash += hash >> 17;
      hash ^= hash << 25;
      hash += hash >> 6;

      return hash;
   }

   private int get16bits(byte[] bytes, int offset) {
      short s = bytes[offset];
      s &= bytes[offset + 1] << 8;
      return s;
   }

   @Override
   public String functionName() {
      return "SuperFastHash";
   }
}

class MurmurHash2 extends HashFunction {
   org.infinispan.commons.hash.MurmurHash2 h = new org.infinispan.commons.hash.MurmurHash2();
   public String functionName() {
      return "MurmurHash2 (neutral)";
   }

   @Override
   public int hash(byte[] payload) {
      return h.hash(payload);
   }
}

class MurmurHash2Compat extends HashFunction {
   org.infinispan.commons.hash.MurmurHash2Compat h = new org.infinispan.commons.hash.MurmurHash2Compat();
   public String functionName() {
      return "MurmurHash2Compat (neutral)";
   }

   @Override
   public int hash(byte[] payload) {
      return h.hash(payload);
   }
}

class MurmurHash3 extends HashFunction {
   org.infinispan.commons.hash.MurmurHash3 h = new org.infinispan.commons.hash.MurmurHash3();
   public String functionName() {
      return "MurmurHash3";
   }

   @Override
   public int hash(byte[] payload) {
      return h.hash(payload);
   }
}
