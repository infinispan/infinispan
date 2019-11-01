package org.infinispan.commons.test;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.infinispan.commons.test.skip.OS;

/**
 * Keep track of running tests and interrupt them if they take more than {@link #MAX_TEST_SECONDS} seconds.
 *
 * @author Dan Berindei
 * @since 9.2
 */
class RunningTestsRegistry {
   private static final long MAX_TEST_SECONDS = Long.parseUnsignedLong(System.getProperty("infinispan.test.maxTestSeconds", "300"));

   private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
         r -> new Thread(r, "RunningTestsRegistry-Worker"));
   private static final Map<Thread, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

   static void unregisterThreadWithTest() {
      ScheduledFuture<?> killTask = scheduledTasks.remove(Thread.currentThread());
      if (killTask != null) {
         killTask.cancel(false);
      }
   }

   static void registerThreadWithTest(String testName, String simpleName) {
      Thread testThread = Thread.currentThread();
      ScheduledFuture<?> future = executor.schedule(
         () -> killLongTest(testThread, testName, simpleName), MAX_TEST_SECONDS, SECONDS);
      scheduledTasks.put(testThread, future);
   }

   private static void killLongTest(Thread testThread, String testName, String simpleName) {
      RuntimeException exception =
         new RuntimeException(String.format("Test timed out after %d seconds", MAX_TEST_SECONDS));
      exception.setStackTrace(testThread.getStackTrace());
      TestSuiteProgress.fakeTestFailure(testName, exception);
      writeJUnitReport(testName, exception);

      List<String> pids = collectChildProcesses();

      String safeTestName = simpleName.replaceAll("[^a-zA-Z0-9=]", "_");
      dumpThreads(safeTestName, pids);

      killTest(testThread, pids);
   }

   private static void writeJUnitReport(String testName, RuntimeException exception) {
      try {
         File reportsDir = new File("target/surefire-reports");
         if (!reportsDir.exists() && !reportsDir.mkdirs()) {
            throw new IOException("Cannot create report directory " + reportsDir.getAbsolutePath());
         }
         PolarionJUnitXMLWriter writer = new PolarionJUnitXMLWriter(
            new File(reportsDir, "TEST-" + testName + "-Timeout.xml"));
         String property = System.getProperty("infinispan.modulesuffix");
         String moduleName = property != null ? property.substring(1) : "";
         writer.start(moduleName, 1, 0, 1, 0, false);

         StringWriter exceptionWriter = new StringWriter();
         exception.printStackTrace(new PrintWriter(exceptionWriter));
         writer.writeTestCase("Timeout", testName, 0, PolarionJUnitXMLWriter.Status.FAILURE,
                              exceptionWriter.toString(), exception.getClass().getName(), exception.getMessage());

         writer.close();
      } catch (Exception e) {
         throw new RuntimeException("Error reporting thread leaks", e);
      }
   }

   private static List<String> collectChildProcesses() {
      try {
         String jvmName = ManagementFactory.getRuntimeMXBean().getName();
         String ppid = jvmName.split("@")[0];

         List<String> pids = new ArrayList<>(Collections.singletonList(ppid));
         int index = 0;
         while (index < pids.size()) {
            String pid = pids.get(index);
            if (OS.getCurrentOs() != OS.WINDOWS) {
               // List pid and command name of child processes
               Process ps = new ProcessBuilder()
                               .command("ps", "-o", "pid=,comm=", "--ppid", pid)
                               .start();
               try (BufferedReader psOutput = new BufferedReader(new InputStreamReader(ps.getInputStream()))) {
                  psOutput.lines().forEach(line -> {
                     // Add children to the list excluding the ps command we just ran
                     String[] pidAndCommand = line.trim().split("\\s+");
                     if (!"ps".equals(pidAndCommand[1])) {
                        pids.add(pidAndCommand[0]);
                     }
                  });
               }

               ps.waitFor(10, SECONDS);
            }

            index++;
         }
         return pids;
      } catch (Exception e) {
         System.err.println("Error collecting child processes:");
         e.printStackTrace(System.err);
         return Collections.emptyList();
      }
   }

   private static void dumpThreads(String safeTestName, List<String> pids) {
      try {
         String extension = OS.getCurrentOs() == OS.WINDOWS ? ".exe" : "";
         String javaHome = System.getProperty("java.home");
         File jstackFile = new File(javaHome, "bin/jstack" + extension);
         if (!jstackFile.canExecute()) {
            jstackFile = new File(javaHome, "../bin/jstack" + extension);
         }
         LocalDateTime now = LocalDateTime.now();
         if (jstackFile.canExecute() && !pids.isEmpty()) {
            for (String pid : pids) {
               File dumpFile = new File(String.format("threaddump-%1$s-%2$tY%2$tm%2$td-%2$tH%2$tM-%3$s.log",
                                                      safeTestName, now, pid));
               System.err.printf("Dumping threads of process %s to %s\n", pid, dumpFile.getAbsolutePath());
               Process jstack = new ProcessBuilder()
                                   .command(jstackFile.getAbsolutePath(), pid)
                                   .redirectOutput(dumpFile)
                                   .start();
               jstack.waitFor(10, SECONDS);
            }
         } else {
            File dumpFile = new File(String.format("threaddump-%1$s-%2$tY%2$tm%2$td-%2$tH%2$tM.log",
                                                   safeTestName, now));
            System.err.printf("Cannot find jstack in %s, programmatically dumping thread stacks of testsuite process to %s\n", javaHome, dumpFile.getAbsolutePath());
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            try (PrintWriter writer = new PrintWriter(new FileWriter(dumpFile))) {
               // 2019-02-05 14:03:11
               writer.printf("%1$tF %1$tT\nTest thread dump:\n\n", now);
               ThreadInfo[] threads = threadMXBean.dumpAllThreads(true, true);
               for (ThreadInfo thread : threads) {
                  dumpThread(writer, thread);
               }
            }
         }
      } catch (Exception e) {
         System.err.println("Error dumping threads:");
         e.printStackTrace(System.err);
      }
   }

   private static void killTest(Thread testThread, List<String> pids) {
      try {
         // Interrupt the test thread
         testThread.interrupt();
         System.err.printf("Interrupted thread %s (%d).\n", testThread.getName(), testThread.getId());

         testThread.join(SECONDS.toMillis(1));
         if (testThread.isAlive()) {
            // Thread.interrupt() doesn't work if the thread is waiting to enter a synchronized block or in lock()
            // Thread.stop() works for lock(), but not if the thread is waiting to enter a synchronized block
            // So we just kill the fork and its children instead
            Process kill;
            List<String> command;
            if (OS.getCurrentOs() == OS.WINDOWS) {
               command = new ArrayList<>(Arrays.asList("taskkill", "/t", "/f"));
               for (String pid : pids) {
                  command.add("/pid");
                  command.add(pid);
               }
               kill = new ProcessBuilder()
                         .command(command)
                         .start();
            } else {
               command = new ArrayList<>(Collections.singletonList("kill"));
               command.addAll(pids);
               kill = new ProcessBuilder()
                         .command(command)
                         .start();
            }
            kill.waitFor(10, SECONDS);
            if (kill.exitValue() == 0) {
               System.err.printf("Killed processes %s\n", String.join(" ", pids));
            } else {
               System.err.printf("Failed to kill processes, exit code %d from command %s\n", kill.exitValue(), command);
            }
         }
      } catch (Exception e) {
         System.err.println("Error killing test:");
         e.printStackTrace(System.err);
      }
   }

   private static void dumpThread(PrintWriter writer, ThreadInfo thread) {
      // "management I/O-2" tid=0x00007fe6a8134000 runnable
      // [0x00007fe64e4db000]
      //    java.lang.Thread.State:RUNNABLE
      //         - waiting to lock <0x00007fa12e5a5d40> (a java.lang.Object)
      //         - waiting on <0x00007fb2c4017ba0> (a java.util.LinkedList)
      //         - parking to wait for  <0x00007fc96bd87cf0> (a java.util.concurrent.CompletableFuture$Signaller)
      //         - locked <0x00007fb34c037e20> (a com.arjuna.ats.arjuna.coordinator.TransactionReaper)
      writer.printf("\"%s\" #%s prio=0 tid=0x%x nid=NA %s\n", thread.getThreadName(), thread.getThreadId(),
                    thread.getThreadId(), thread.getThreadState().toString().toLowerCase());
      writer.printf("   java.lang.Thread.State: %s\n", thread.getThreadState());
      LockInfo blockedLock = thread.getLockInfo();
      StackTraceElement[] s = thread.getStackTrace();
      MonitorInfo[] monitors = thread.getLockedMonitors();
      for (int i = 0; i < s.length; i++) {
         StackTraceElement ste = s[i];
         writer.printf("\tat %s\n", ste);
         if (i == 0 && blockedLock != null) {
            boolean parking = ste.isNativeMethod() && ste.getMethodName().equals("park");
            writer.printf("\t- %s <0x%x> (a %s)\n", blockedState(thread, blockedLock, parking),
                          blockedLock.getIdentityHashCode(), blockedLock.getClassName());
         }
         if (monitors != null) {
            for (MonitorInfo monitor : monitors) {
               if (monitor.getLockedStackDepth() == i) {
                  writer.printf("\t- locked <0x%x> (a %s)\n", monitor.getIdentityHashCode(), monitor.getClassName());
               }
            }
         }
      }
      writer.println();

      LockInfo[] synchronizers = thread.getLockedSynchronizers();
      if (synchronizers != null && synchronizers.length > 0) {
         writer.print("\n   Locked ownable synchronizers:\n");
         for (LockInfo synchronizer : synchronizers) {
            writer.printf("\t- <0x%x> (a %s)\n", synchronizer.getIdentityHashCode(), synchronizer.getClassName());
         }
         writer.println();
      }
   }

   private static String blockedState(ThreadInfo thread, LockInfo blockedLock, boolean parking) {
      String state;
      if (blockedLock != null) {
         if (thread.getThreadState().equals(Thread.State.BLOCKED)) {
            state = "waiting to lock";
         } else if (parking) {
            state = "parking to wait for";
         } else {
            state = "waiting on";
         }
      } else {
         state = null;
      }
      return state;
   }

}
