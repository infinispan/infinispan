/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import org.infinispan.config.ConfigurationException;

import java.io.Closeable;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * General utility methods used throughout the Infinispan code base.
 *
 * @author <a href="brian.stansberry@jboss.com">Brian Stansberry</a>
 * @author Galder Zamarreño
 * @since 4.0
 */
public final class Util {

   private static final boolean isArraysDebug = Boolean.getBoolean("infinispan.arrays.debug");

   /**
    * Loads the specified class using this class's classloader, or, if it is <code>null</code> (i.e. this class was
    * loaded by the bootstrap classloader), the system classloader. <p/> If loadtime instrumentation via
    * GenerateInstrumentedClassLoader is used, this class may be loaded by the bootstrap classloader. <p/>
    * If the class is not found, the {@link ClassNotFoundException} is wrapped as a {@link ConfigurationException} and
    * is re-thrown.
    *
    * @param classname name of the class to load
    * @return the class
    */
   public static Class loadClass(String classname) {
      try {
         return loadClassStrict(classname);
      } catch (Exception e) {
         throw new ConfigurationException("Unable to instantiate class " + classname, e);
      }
   }

   /**
    * Similar to {@link #loadClass(String)} except that any {@link ClassNotFoundException}s experienced is propagated
    * to the caller.
    *
    * @param classname name of the class to load
    * @return the class
    * @throws ClassNotFoundException
    */
   public static Class loadClassStrict(String classname) throws ClassNotFoundException {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      if (cl == null)
         cl = ClassLoader.getSystemClassLoader();
      return cl.loadClass(classname);
   }

   private static Method getFactoryMethod(Class c) {
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
    * Any exceptions encountered are wrapped in a {@link ConfigurationException} and rethrown.
    *
    * @param clazz class to instantiate
    * @return an instance of the class
    */
   @SuppressWarnings("unchecked")
   public static <T> T getInstance(Class<T> clazz) {
      try {
         return getInstanceStrict(clazz);
      } catch (IllegalAccessException iae) {
         throw new ConfigurationException("Unable to instantiate class " + clazz.getName(), iae);
      } catch (InstantiationException ie) {
         throw new ConfigurationException("Unable to instantiate class " + clazz.getName(), ie);
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
      return instance == null ? null : instance;
   }

   /**
    * Instantiates a class based on the class name provided.  Instatiation is attempted via an appropriate, static
    * factory method named <tt>getInstance()</tt> first, and failing the existence of an appropriate factory, falls
    * back to an empty constructor.
    * <p />
    * Any exceptions encountered loading and instantiating the class is wrapped in a {@link ConfigurationException}.
    *
    * @param classname class to instantiate
    * @return an instance of classname
    */
   @SuppressWarnings("unchecked")
   public static Object getInstance(String classname) {
      if (classname == null) throw new IllegalArgumentException("Cannot load null class!");
      Class clazz = loadClass(classname);
      return getInstance(clazz);
   }

   /**
    * Similar to {@link #getInstance(String)} except that exceptions are propagated to the caller.
    *
    * @param classname class to instantiate
    * @return an instance of classname
    * @throws ClassNotFoundException
    * @throws InstantiationException
    * @throws IllegalAccessException
    */
   @SuppressWarnings("unchecked")
   public static Object getInstanceStrict(String classname) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
      if (classname == null) throw new IllegalArgumentException("Cannot load null class!");
      Class clazz = loadClassStrict(classname);
      return getInstanceStrict(clazz);
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

   public static InputStream loadResourceAsStream(String resource) {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      InputStream s = cl.getResourceAsStream(resource);
      if (s == null) {
         cl = Util.class.getClassLoader();
         s = cl.getResourceAsStream(resource);
      }
      return s;
   }

   /**
    * Static inner class that holds 3 maps - for data added, removed and modified.
    */
   public static class MapModifications {
      public final Map<Object, Object> addedEntries = new HashMap<Object, Object>();
      public final Map<Object, Object> removedEntries = new HashMap<Object, Object>();
      public final Map<Object, Object> modifiedEntries = new HashMap<Object, Object>();


      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         MapModifications that = (MapModifications) o;

         if (addedEntries != null ? !addedEntries.equals(that.addedEntries) : that.addedEntries != null) return false;
         if (modifiedEntries != null ? !modifiedEntries.equals(that.modifiedEntries) : that.modifiedEntries != null)
            return false;
         if (removedEntries != null ? !removedEntries.equals(that.removedEntries) : that.removedEntries != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result;
         result = (addedEntries != null ? addedEntries.hashCode() : 0);
         result = 31 * result + (removedEntries != null ? removedEntries.hashCode() : 0);
         result = 31 * result + (modifiedEntries != null ? modifiedEntries.hashCode() : 0);
         return result;
      }

      @Override
      public String toString() {
         return "Added Entries " + addedEntries + " Removed Entries " + removedEntries + " Modified Entries " + modifiedEntries;
      }
   }

   public static String prettyPrintTime(long time, TimeUnit unit) {
      return prettyPrintTime(unit.toMillis(time));
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

      StringBuilder value = new StringBuilder(String.valueOf(message));
      for (int i = 0; i < params.length; i++) {
         String placeholder = "{" + i + "}";
         int phIndex;
         if ((phIndex = value.indexOf(placeholder)) > -1) {
            value = value.replace(phIndex, phIndex + placeholder.length(), String.valueOf(params[i]));
         }
      }
      return value.toString();
   }

   public static String printArray(byte[] array, boolean withHash) {
      if (array == null) return "null";
      StringBuilder sb = new StringBuilder();
      sb.append("ByteArray{size=").append(array.length);
      if (withHash)
         sb.append(", hashCode=").append(Integer.toHexString(array.hashCode()));

      sb.append(", array=");
      if (isArraysDebug) {
         sb.append(Arrays.toString(array));
      } else {
         sb.append("[");
         int length = array.length < 10 ? array.length : 10;
         for (int i = 0; i < length; i++)
            sb.append(array[i]).append(", ");
         sb.append("..]");
      }
      sb.append("}");

      return sb.toString();
   }

   public static String padString(String s, int minWidth) {
      if (s.length() < minWidth) {
         StringBuilder sb = new StringBuilder(s);
         while (sb.length() < minWidth) sb.append(" ");
         return sb.toString();
      }
      return s;
   }

   /**
    * Releases a lock and swallows any IllegalMonitorStateExceptions - so it is safe to call this method even if the
    * lock is not locked, or not locked by the current thread.
    *
    * @param toRelease lock to release
    */
   public static final void safeRelease(Lock toRelease) {
      if (toRelease != null) {
         try {
            toRelease.unlock();
         } catch (IllegalMonitorStateException imse) {
            // Perhaps the caller hadn't acquired the lock after all.
         }
      }
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
      for (int i = 0; i < locks.length; i++) {
          LockInfo li = locks[i];
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

}
