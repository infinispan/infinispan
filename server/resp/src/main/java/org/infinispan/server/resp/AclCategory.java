package org.infinispan.server.resp;

public final class AclCategory {
   public static long KEYSPACE = 1L;
   public static long READ = 1L << 1;
   public static long WRITE = 1L << 2;
   public static long SET = 1L << 3;
   public static long SORTEDSET = 1L << 4;
   public static long LIST = 1L << 5;
   public static long HASH = 1L << 6;
   public static long STRING = 1L << 7;
   public static long BITMAP = 1L << 8;
   public static long HYPERLOGLOG = 1L << 9;
   public static long GEO = 1L << 10;
   public static long STREAM = 1L << 11;
   public static long PUBSUB = 1L << 12;
   public static long ADMIN = 1L << 13;
   public static long FAST = 1L << 14;
   public static long SLOW = 1L << 15;
   public static long BLOCKING = 1L << 16;
   public static long DANGEROUS = 1L << 17;
   public static long CONNECTION = 1L << 18;
   public static long TRANSACTION = 1L << 19;
   public static long SCRIPTING = 1L << 20;
}
