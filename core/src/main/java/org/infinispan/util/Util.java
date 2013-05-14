/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.util;

import org.infinispan.CacheConfigurationException;
import org.infinispan.CacheException;
import org.infinispan.commons.hash.Hash;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.naming.Context;
import java.io.Closeable;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * General utility methods used throughout the Infinispan code base.
 *
 * @author <a href="brian.stansberry@jboss.com">Brian Stansberry</a>
 * @author Galder Zamarreño
 * @since 4.0
 */
public final class Util {

   private static final boolean IS_ARRAYS_DEBUG = Boolean.getBoolean("infinispan.arrays.debug");

   public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
   public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

   private static final Log log = LogFactory.getLog(Util.class);

   /**
    * Current Java vendor. This variable is later used to differentiate LRU implementations
    * for different java vendors.
    */
   private static final String javaVendor = SysPropertyActions.getProperty("java.vendor", "");

   /**
    * <p>
    * Loads the specified class using the passed classloader, or, if it is <code>null</code> the Infinispan classes'
    * classloader.
    * </p>
    *
    * <p>
    * If loadtime instrumentation via GenerateInstrumentedClassLoader is used, this class may be loaded by the bootstrap
    * classloader.
    * </p>
    * <p>
    * If the class is not found, the {@link ClassNotFoundException} or {@link NoClassDefFoundError} is wrapped as a
    * {@link CacheConfigurationException} and is re-thrown.
    * </p>
    *
    * @param classname name of the class to load
    * @param cl the application classloader which should be used to load the class, or null if the class is always packaged with
    *        Infinispan
    * @return the class
    * @throws CacheConfigurationException if the class cannot be loaded
    */
   public static <T> Class<T> loadClass(String classname, ClassLoader cl) {
      try {
         return loadClassStrict(classname, cl);
      } catch (ClassNotFoundException e) {
         throw new CacheConfigurationException("Unable to instantiate class " + classname, e);
      }
   }

   public static ClassLoader[] getClassLoaders(ClassLoader appClassLoader) {
      return new ClassLoader[] {
            appClassLoader,  // User defined classes
            Util.class.getClassLoader(), // Infinispan classes (not always on TCCL [modular env])
            ClassLoader.getSystemClassLoader() // Used when load time instrumentation is in effect
            };
   }

   /**
    * <p>
    * Loads the specified class using the passed classloader, or, if it is <code>null</code> the Infinispan classes' classloader.
    * </p>
    *
    * <p>
    * If loadtime instrumentation via GenerateInstrumentedClassLoader is used, this class may be loaded by the bootstrap classloader.
    * </p>
    *
    * @param classname name of the class to load
    * @return the class
    * @param userClassLoader the application classloader which should be used to load the class, or null if the class is always packaged with
    *        Infinispan
    * @throws ClassNotFoundException if the class cannot be loaded
    */
   @SuppressWarnings("unchecked")
   public static <T> Class<T> loadClassStrict(String classname, ClassLoader userClassLoader) throws ClassNotFoundException {
      ClassLoader[] cls = getClassLoaders(userClassLoader);
         ClassNotFoundException e = null;
         NoClassDefFoundError ne = null;
         for (ClassLoader cl : cls)  {
            if (cl == null)
               continue;

            try {
               return (Class<T>) Class.forName(classname, true, cl);
            } catch (ClassNotFoundException ce) {
               e = ce;
            } catch (NoClassDefFoundError ce) {
               ne = ce;
            }
         }

         if (e != null)
            throw e;
         else if (ne != null) {
            // Before we wrap this, make sure we appropriately log this.
            log.unableToLoadClass(classname, Arrays.toString(cls), ne);
            throw new ClassNotFoundException(classname, ne);
         }
         else
            throw new IllegalStateException();
   }

   private static Method getFactoryMethod(Class<?> c) {
      for (Method m : c.getMethods()) {
         if (m.getName().equals("getInstance") && m.getParameterTypes().length == 0 && Modifier.isStatic(m.getModifiers()))
            return m;
      }
      return null;
   }

   /**
    * Instantiates a class by first attempting a static <i>factory method</i> named <tt>getInstance()</tt> on the class
    * and then falling back to an empty constructor.
    * <p/>
    * Any exceptions encountered are wrapped in a {@link CacheConfigurationException} and rethrown.
    *
    * @param clazz class to instantiate
    * @return an instance of the class
    */
   public static <T> T getInstance(Class<T> clazz) {
      try {
         return getInstanceStrict(clazz);
      } catch (IllegalAccessException iae) {
         throw new CacheConfigurationException("Unable to instantiate class " + clazz.getName(), iae);
      } catch (InstantiationException ie) {
         throw new CacheConfigurationException("Unable to instantiate class " + clazz.getName(), ie);
      }
   }

   /**
    * Similar to {@link #getInstance(Class)} except that exceptions are propagated to the caller.
    *
    * @param clazz class to instantiate
    * @return an instance of the class
    * @throws IllegalAccessException
    * @throws InstantiationException
    */
   @SuppressWarnings("unchecked")
   public static <T> T getInstanceStrict(Class<T> clazz) throws IllegalAccessException, InstantiationException {
      // first look for a getInstance() constructor
      T instance = null;
      try {
         Method factoryMethod = getFactoryMethod(clazz);
         if (factoryMethod != null) instance = (T) factoryMethod.invoke(null);
      }
      catch (Exception e) {
         // no factory method or factory method failed.  Try a constructor.
         instance = null;
      }
      if (instance == null) {
         instance = clazz.newInstance();
      }
      return instance;
   }

   /**
    * Instantiates a class based on the class name provided.  Instantiation is attempted via an appropriate, static
    * factory method named <tt>getInstance()</tt> first, and failing the existence of an appropriate factory, falls
    * back to an empty constructor.
    * <p />
    * Any exceptions encountered loading and instantiating the class is wrapped in a {@link CacheConfigurationException}.
    *
    * @param classname class to instantiate
    * @return an instance of classname
    */
   public static <T> T getInstance(String classname, ClassLoader cl) {
      if (classname == null) throw new IllegalArgumentException("Cannot load null class!");
      Class<T> clazz = loadClass(classname, cl);
      return getInstance(clazz);
   }

   /**
    * Similar to {@link #getInstance(String, ClassLoader)} except that exceptions are propagated to the caller.
    *
    * @param classname class to instantiate
    * @return an instance of classname
    * @throws ClassNotFoundException
    * @throws InstantiationException
    * @throws IllegalAccessException
    */
   public static <T> T getInstanceStrict(String classname, ClassLoader cl) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
      if (classname == null) throw new IllegalArgumentException("Cannot load null class!");
      Class<T> clazz = loadClassStrict(classname, cl);
      return getInstanceStrict(clazz);
   }

   /**
    * Clones parameter x of type T with a given Marshaller reference;
    *
    *
    * @return a deep clone of an object parameter x
    */
   @SuppressWarnings("unchecked")
   public static <T> T cloneWithMarshaller(Marshaller marshaller, T x){
      if (marshaller == null)
         throw new IllegalArgumentException("Cannot use null Marshaller for clone");

      byte[] byteBuffer;
      try {
         byteBuffer = marshaller.objectToByteBuffer(x);
         return (T) marshaller.objectFromByteBuffer(byteBuffer);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }


   /**
    * Prevent instantiation
    */
   private Util() {
   }

   /**
    * Null-safe equality test.
    *
    * @param a first object to compare
    * @param b second object to compare
    * @return true if the objects are equals or both null, false otherwise.
    */
   public static boolean safeEquals(Object a, Object b) {
      return (a == b) || (a != null && a.equals(b));
   }

   public static String prettyPrintTime(long time, TimeUnit unit) {
      return prettyPrintTime(unit.toMillis(time));
   }

   /**
    * {@link System#nanoTime()} is less expensive than {@link System#currentTimeMillis()} and better suited
    * to measure time intervals. It's NOT suited to know the current time, for example to be compared
    * with the time of other nodes.
    * @return the value of {@link System#nanoTime()}, but converted in Milliseconds.
    */
   public static final long currentMillisFromNanotime() {
      return TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
   }

   /**
    * Prints a time for display
    *
    * @param millis time in millis
    * @return the time, represented as millis, seconds, minutes or hours as appropriate, with suffix
    */
   public static String prettyPrintTime(long millis) {
      if (millis < 1000) return millis + " milliseconds";
      NumberFormat nf = NumberFormat.getNumberInstance();
      nf.setMaximumFractionDigits(2);
      double toPrint = ((double) millis) / 1000;
      if (toPrint < 300) {
         return nf.format(toPrint) + " seconds";
      }

      toPrint = toPrint / 60;

      if (toPrint < 120) {
         return nf.format(toPrint) + " minutes";
      }

      toPrint = toPrint / 60;

      return nf.format(toPrint) + " hours";
   }

   public static void close(Closeable cl) {
      if (cl == null) return;
      try {
         cl.close();
      } catch (Exception e) {
      }
   }

   public static void close(Socket s) {
      if (s == null) return;
      try {
         s.close();
      } catch (Exception e) {
      }
   }

   public static void close(Closeable... cls) {
      for (Closeable cl : cls) {
         close(cl);
      }
   }

   public static void close(Context ctx) {
      if (ctx == null) return;
      try {
         ctx.close();
      } catch (Exception e) {
      }
   }

   public static void flushAndCloseStream(OutputStream o) {
      if (o == null) return;
      try {
         o.flush();
      } catch (Exception e) {

      }

      try {
         o.close();
      } catch (Exception e) {

      }
   }

   public static void flushAndCloseOutput(ObjectOutput o) {
      if (o == null) return;
      try {
         o.flush();
      } catch (Exception e) {

      }

      try {
         o.close();
      } catch (Exception e) {

      }
   }

   public static String formatString(Object message, Object... params) {
      if (params.length == 0) return message == null ? "null" : message.toString();

      return String.format(message.toString(), params);
   }

   public static String toStr(Object o) {
      if (o instanceof byte[]) {
         byte[] array = (byte[]) o;
         StringBuilder sb = new StringBuilder();
         sb.append("[B0x");
         if (IS_ARRAYS_DEBUG) {
            // Convert the entire byte array
            sb.append(toHexString(array));
         } else {
            // Pick the first 8 characters and convert that part
            sb.append(toHexString(array, 8));
            sb.append("..(");
            sb.append(array.length);
            sb.append(')');
         }
         return sb.toString();
      } else if (o == null) {
         return "null";

      } else {
         return o.toString();
      }
   }

   public static <E> String toStr(Collection<E> collection) {
      if (collection == null)
         return "[]";

      Iterator<E> i = collection.iterator();
      if (!i.hasNext())
         return "[]";

      StringBuilder sb = new StringBuilder();
      sb.append('[');
      for (;;) {
         E e = i.next();
         sb.append(e == collection ? "(this Collection)" : toStr(e));
         if (! i.hasNext())
            return sb.append(']').toString();
         sb.append(", ");
      }
   }

   public static String printArray(byte[] array, boolean withHash) {
      if (array == null) return "null";
      StringBuilder sb = new StringBuilder();
      sb.append("ByteArray{size=").append(array.length);
      if (withHash) {
         sb.append(", hashCode=").append(
               Integer.toHexString(Arrays.hashCode(array)));
      }

      sb.append(", array=0x");
      if (IS_ARRAYS_DEBUG) {
         // Convert the entire byte array
         sb.append(toHexString(array));
      } else {
         // Pick the first 8 characters and convert that part
         sb.append(toHexString(array, 8));
         sb.append("..");
      }
      sb.append("}");

      return sb.toString();
   }

   public static String toHexString(byte input[]) {
      return toHexString(input, input.length);
   }

   public static String toHexString(byte input[], int limit) {
      int i = 0;
      if (input == null || input.length <= 0)
         return null;

      char lookup[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                       '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

      char[] result = new char[(input.length < limit ? input.length : limit) * 2];

      while (i < limit && i < input.length) {
         result[2*i] = lookup[(input[i] >> 4) & 0x0F];
         result[2*i+1] = lookup[(input[i] & 0x0F)];
         i++;
      }
      return String.valueOf(result);
   }

   public static String padString(String s, int minWidth) {
      if (s.length() < minWidth) {
         StringBuilder sb = new StringBuilder(s);
         while (sb.length() < minWidth) sb.append(" ");
         return sb.toString();
      }
      return s;
   }

   private static String INDENT = "    ";

   public static String threadDump() {
      StringBuilder threadDump = new StringBuilder();
      ThreadMXBean threadMx = ManagementFactory.getThreadMXBean();
      if (threadMx.isObjectMonitorUsageSupported() && threadMx.isSynchronizerUsageSupported()) {
         // Print lock info if, and only if, both object monitor usage and synchronizer usage are supported.
          dumpThreadInfo(threadDump, true, threadMx);
      } else {
         dumpThreadInfo(threadDump, false, threadMx);
      }
      return threadDump.toString();
   }

   private static void dumpThreadInfo(StringBuilder threadDump, boolean withLocks, ThreadMXBean threadMx) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      String timestamp = dateFormat.format(new Date());
      threadDump.append(timestamp);
      threadDump.append("\nFull thread dump ");
      threadDump.append("\n");

      if (withLocks) {
         ThreadInfo[] threadInfos = threadMx.dumpAllThreads(true, true);
         for (ThreadInfo threadInfo : threadInfos) {
            printThreadInfo(threadInfo, threadDump);
            LockInfo[] syncs = threadInfo.getLockedSynchronizers();
            printLockInfo(syncs, threadDump);
         }
         threadDump.append("\n");
      } else {
         long[] threadIds= threadMx.getAllThreadIds();
         ThreadInfo[] threadInfos = threadMx.getThreadInfo(threadIds, Integer.MAX_VALUE);
         for (ThreadInfo threadInfo : threadInfos)
            printThreadInfo(threadInfo, threadDump);
      }
   }

   private static void printThreadInfo(ThreadInfo threadInfo, StringBuilder threadDump) {
      // Print thread information
      printThread(threadInfo, threadDump);
      // print stack trace with locks
      StackTraceElement[] stacktrace = threadInfo.getStackTrace();
      MonitorInfo[] monitors = threadInfo.getLockedMonitors();
      for (int i = 0; i < stacktrace.length; i++) {
          StackTraceElement ste = stacktrace[i];
          threadDump.append(INDENT + "at " + ste.toString());
          threadDump.append("\n");
          for (int j = 1; j < monitors.length; j++) {
              MonitorInfo mi = monitors[j];
              if (mi.getLockedStackDepth() == i) {
                  threadDump.append(INDENT + "  - locked " + mi);
                  threadDump.append("\n");
              }
          }
      }
      threadDump.append("\n");
   }

   private static void printLockInfo(LockInfo[] locks, StringBuilder threadDump) {
      threadDump.append(INDENT + "Locked synchronizers: count = " + locks.length);
      threadDump.append("\n");
      for (LockInfo li : locks) {
         threadDump.append(INDENT + "  - " + li);
         threadDump.append("\n");
      }
      threadDump.append("\n");
   }

   private static void printThread(ThreadInfo threadInfo, StringBuilder threadDump) {
      StringBuilder sb = new StringBuilder(
            "\"" + threadInfo.getThreadName() + "\"" +
            " nid=" + threadInfo.getThreadId() + " state=" +
            threadInfo.getThreadState());
      if (threadInfo.getLockName() != null && threadInfo.getThreadState() != Thread.State.BLOCKED) {
          String[] lockInfo = threadInfo.getLockName().split("@");
          sb.append("\n" + INDENT +"- waiting on <0x" + lockInfo[1] + "> (a " + lockInfo[0] + ")");
          sb.append("\n" + INDENT +"- locked <0x" + lockInfo[1] + "> (a " + lockInfo[0] + ")");
      } else if (threadInfo.getLockName() != null && threadInfo.getThreadState() == Thread.State.BLOCKED) {
          String[] lockInfo = threadInfo.getLockName().split("@");
          sb.append("\n" + INDENT +"- waiting to lock <0x" + lockInfo[1] + "> (a " + lockInfo[0] + ")");
      }
      if (threadInfo.isSuspended())
          sb.append(" (suspended)");

      if (threadInfo.isInNative())
          sb.append(" (running in native)");

      threadDump.append(sb.toString());
      threadDump.append("\n");
      if (threadInfo.getLockOwnerName() != null) {
           threadDump.append(INDENT + " owned by " + threadInfo.getLockOwnerName() +
                              " id=" + threadInfo.getLockOwnerId());
           threadDump.append("\n");
      }
   }

   public static CacheException rewrapAsCacheException(Throwable t) {
      if (t instanceof CacheException)
         return (CacheException) t;
      else
         return new CacheException(t);
   }

   public static <T> Set<T> asSet(T... a) {
      if (a.length > 1)
         return new HashSet<T>(Arrays.<T>asList(a));
      else
         return Collections.singleton(a[0]);
   }

   /**
    * Prints the identity hash code of the object passed as parameter
    * in an hexadecimal format in order to safe space.
    */
   public static String hexIdHashCode(Object o) {
      return Integer.toHexString(System.identityHashCode(o));
   }

   static final String HEX_VALUES = "0123456789ABCDEF";

   public static String hexDump(byte[] buffer) {
      StringBuilder buf = new StringBuilder(buffer.length << 1);
      for (byte b : buffer)
         addHexByte(buf, b);

      return buf.toString();
   }

   public static String hexDump(ByteBuffer buffer) {
      byte[] data = new byte[buffer.remaining()];
      int pos = buffer.position();
      buffer.get(data);
      buffer.position(pos);
      StringBuilder buf = new StringBuilder(buffer.remaining() + 22);
      for (byte b : data)
         addHexByte(buf, b);

      return buf.toString();
   }

   private static void addHexByte(StringBuilder buf, byte b) {
      buf.append(HEX_VALUES.charAt((b & 0xF0) >> 4))
         .append(HEX_VALUES.charAt((b & 0x0F)));
   }

   public static Double constructDouble(Class<?> type, Object o) {
      if (type.equals(Long.class) || type.equals(long.class))
         return Double.valueOf((Long) o);
      else if (type.equals(Double.class) || type.equals(double.class))
         return (Double) o;
      else if (type.equals(Integer.class) || type.equals(int.class))
         return Double.valueOf((Integer) o);
      else if (type.equals(String.class))
         return Double.valueOf((String) o);

      throw new IllegalStateException(String.format("Expected a value that can be converted into a double: type=%s, value=%s", type, o));
   }

   /**
    * Applies the given hash function to the hash code of a given object, and then normalizes it to ensure a positive
    * value is always returned.
    * @param object to hash
    * @param hashFct hash function to apply
    * @return a non-null, non-negative normalized hash code for a given object
    */
   public static int getNormalizedHash(Object object, Hash hashFct) {
      // make sure no negative numbers are involved.
      return hashFct.hash(object) & Integer.MAX_VALUE;
   }

   public static boolean isIBMJavaVendor() {
      return javaVendor.toLowerCase().contains("ibm");
   }

   public static String join(List<String> strings, String separator) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;

      for (String string : strings) {
         if (!first) {
            sb.append(separator);
         } else {
            first = false;
         }
         sb.append(string);
      }

      return sb.toString();
   }
}
