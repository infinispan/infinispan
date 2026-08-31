package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.sun.management.GarbageCollectionNotificationInfo;

/**
 * Reproducer for <a href="https://github.com/infinispan/infinispan/issues/18021">ISPN-18021</a>:
 * {@link MemoryMonitor} raising false GC duration ({@code ISPN000978}) and GC pressure
 * ({@code ISPN000979}) alerts on generational ZGC.
 * <p>
 * <b>Root cause.</b> Concurrent collectors expose a dedicated {@link GarbageCollectorMXBean} for the
 * full concurrent cycle whose {@link com.sun.management.GcInfo#getDuration()} reports the
 * <em>wall-clock</em> elapsed time of the cycle (potentially tens of seconds on a healthy heap),
 * not the stop-the-world pause time. That bean fires a {@code GARBAGE_COLLECTION_NOTIFICATION} with
 * the action string {@code "end of GC cycle"}. Before the fix, {@code MemoryMonitor} recorded that
 * wall-clock duration as if it were a pause, inflating both the duration warning and the rolling
 * GC-pressure ratio. The fix filters {@code "end of GC cycle"} notifications
 * ({@link MemoryMonitor#isConcurrentGcAction(String)}) before recording them.
 * <p>
 * <b>Why this test forks a JVM per collector.</b> The problematic notification only exists when the
 * relevant collector is actually running, and a synthetic {@code GarbageCollectionNotificationInfo}
 * cannot be constructed ({@code GcInfo} has no public constructor). So for every collector supported
 * by the running JDK we fork a child JVM with that collector, drive real garbage collections, and
 * verify — against the <em>real</em> JMX listener path wired up by {@link MemoryMonitor} — that:
 * <ul>
 *    <li>the monitor records <em>exactly</em> the non-{@code "end of GC cycle"} notifications, i.e.
 *    every concurrent-cycle notification is excluded from duration and pressure accounting; and</li>
 *    <li>no false GC pressure alert fires on the healthy, lightly-loaded heap.</li>
 * </ul>
 * <p>
 * <b>Empirically-observed action strings (Temurin 25).</b> Both ZGC <em>and</em> Shenandoah emit
 * {@code "end of GC cycle"} for their concurrent-cycle bean and report their real STW pauses through
 * a separate bean ({@code "end of GC pause"} for ZGC; {@code "Init Mark"}/{@code "Final Mark"}/... for
 * Shenandoah). The stop-the-world collectors (Serial, Parallel, G1) never emit {@code "end of GC
 * cycle"}, so the filter is a no-op for them. This test asserts that the concurrent-cycle notification
 * was actually observed for ZGC and Shenandoah, so it genuinely exercises the buggy path rather than
 * silently passing.
 */
public class MemoryMonitorGcNotificationTest {

   /** Action string emitted by the concurrent-cycle bean of ZGC and Shenandoah. */
   private static final String CONCURRENT_CYCLE_ACTION = "end of GC cycle";

   /**
    * @param name         human-readable collector name (for test reporting)
    * @param jvmFlags     the {@code -XX} flags that select this collector
    * @param concurrent   {@code true} if this collector is expected to emit a concurrent-cycle
    *                     notification ({@code "end of GC cycle"}) that must be filtered
    */
   private record GcConfig(String name, List<String> jvmFlags, boolean concurrent) {
      @Override
      public String toString() {
         return name;
      }
   }

   private static List<GcConfig> candidateCollectors() {
      return List.of(
            new GcConfig("Serial", List.of("-XX:+UseSerialGC"), false),
            new GcConfig("Parallel", List.of("-XX:+UseParallelGC"), false),
            new GcConfig("G1", List.of("-XX:+UseG1GC"), false),
            new GcConfig("ZGC", List.of("-XX:+UseZGC"), true),
            new GcConfig("Shenandoah", List.of("-XX:+UseShenandoahGC"), true));
   }

   /** Only feed the test collectors the running JDK actually supports. */
   static List<GcConfig> supportedCollectors() {
      List<GcConfig> supported = new ArrayList<>();
      for (GcConfig cfg : candidateCollectors()) {
         if (isCollectorSupported(cfg)) {
            supported.add(cfg);
         }
      }
      return supported;
   }

   private static boolean isCollectorSupported(GcConfig cfg) {
      try {
         List<String> cmd = new ArrayList<>();
         cmd.add(javaBinary());
         cmd.addAll(cfg.jvmFlags());
         cmd.add("-version");
         Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
         boolean done = p.waitFor(30, TimeUnit.SECONDS);
         if (!done) {
            p.destroyForcibly();
            return false;
         }
         return p.exitValue() == 0;
      } catch (Exception e) {
         return false;
      }
   }

   @ParameterizedTest(name = "{0}")
   @MethodSource("supportedCollectors")
   public void testConcurrentCycleNotificationsAreExcluded(GcConfig cfg) throws Exception {
      List<String> cmd = new ArrayList<>();
      cmd.add(javaBinary());
      cmd.addAll(cfg.jvmFlags());
      cmd.add("-Xmx256m");
      cmd.add("-Xms256m");
      cmd.add("-cp");
      cmd.add(System.getProperty("java.class.path"));
      cmd.add(Child.class.getName());

      Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
      List<String> lines = new ArrayList<>();
      Map<String, String> kv = new HashMap<>();
      try (BufferedReader r = new BufferedReader(
            new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
         String line;
         while ((line = r.readLine()) != null) {
            lines.add(line);
            int eq = line.indexOf('=');
            if (eq > 0 && !line.contains(" ")) {
               kv.put(line.substring(0, eq), line.substring(eq + 1));
            }
         }
      }
      boolean exited = p.waitFor(60, TimeUnit.SECONDS);
      if (!exited) {
         p.destroyForcibly();
      }

      String output = String.join("\n", lines);
      assumeTrue(!"true".equals(kv.get("NO_GC_OBSERVED")),
            () -> cfg.name() + ": child JVM observed no GC activity, cannot evaluate\n" + output);

      // The real MemoryMonitor listener must have recorded exactly the notifications that are NOT
      // "end of GC cycle" — i.e. every concurrent-cycle notification was filtered out. This is the
      // deterministic core of the reproducer: on the pre-fix code the recorded count would include
      // the concurrent-cycle events and would not match.
      long recorded = Long.parseLong(kv.getOrDefault("MONITOR_RECORDED_DELTA", "-1"));
      long keptExpected = Long.parseLong(kv.getOrDefault("KEPT_NOTIFICATION_DELTA", "-2"));
      assertEquals(keptExpected, recorded,
            () -> cfg.name() + ": monitor should record every non-\"" + CONCURRENT_CYCLE_ACTION
                  + "\" notification and no concurrent-cycle notification\n" + output);

      // No false GC pressure alert on a healthy, lightly-loaded heap.
      assertEquals("0", kv.get("GC_PRESSURE_HIGH_CALLBACKS"),
            () -> cfg.name() + ": no GC pressure alert should fire on a healthy heap\n" + output);
      assertEquals("false", kv.get("MONITOR_GC_PRESSURE"),
            () -> cfg.name() + ": monitor must not report GC pressure on a healthy heap\n" + output);

      // For the concurrent collectors, prove the buggy path was actually exercised: the
      // "end of GC cycle" notification must have been observed (and its wall-clock duration is what
      // would previously have been mis-recorded).
      if (cfg.concurrent()) {
         assertTrue("true".equals(kv.get("CONCURRENT_CYCLE_OBSERVED")),
               () -> cfg.name() + ": expected a \"" + CONCURRENT_CYCLE_ACTION
                     + "\" notification but none was observed; test would not exercise the fix\n"
                     + output);
      }

      assertEquals("PASS", kv.get("RESULT"),
            () -> cfg.name() + ": child reproducer did not pass\n" + output);
   }

   private static String javaBinary() {
      return System.getProperty("java.home") + java.io.File.separator + "bin"
            + java.io.File.separator + "java";
   }

   /**
    * Runs inside a forked JVM under a specific collector. Wires up a real {@link MemoryMonitor}
    * alongside an independent raw JMX listener (the source of truth for what the JVM actually
    * emitted), drives real GCs, then prints a machine-readable summary consumed by the parent test.
    */
   public static class Child {
      private static final long QUIET_MS = 1500;

      public static void main(String[] args) throws Exception {
         // Per-action [count, maxDurationMs], updated from a raw listener registered on every GC bean.
         Map<String, long[]> actionStats = new ConcurrentHashMap<>();
         AtomicLong lastNotificationNanos = new AtomicLong(System.nanoTime());

         NotificationListener raw = (n, hb) -> {
            if (GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(n.getType())) {
               GarbageCollectionNotificationInfo info =
                     GarbageCollectionNotificationInfo.from((CompositeData) n.getUserData());
               long dur = info.getGcInfo().getDuration();
               actionStats.compute(info.getGcAction(), (k, v) ->
                     v == null ? new long[]{1, dur} : new long[]{v[0] + 1, Math.max(v[1], dur)});
               lastNotificationNanos.set(System.nanoTime());
            }
         };
         List<NotificationEmitter> emitters = new ArrayList<>();
         for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (bean instanceof NotificationEmitter emitter) {
               emitter.addNotificationListener(raw, null, null);
               emitters.add(emitter);
            }
         }

         // Low duration threshold and a window that never evicts, so that if concurrent-cycle
         // wall-clock durations were (incorrectly) recorded they would visibly inflate the metrics.
         MemoryMonitor monitor = new MemoryMonitor(0.85, 200, 0.20, 600_000);
         AtomicInteger pressureHighCallbacks = new AtomicInteger();
         monitor.addListener(new MemoryMonitor.Listener() {
            @Override
            public void onMemoryLow() {}

            @Override
            public void onMemoryRecovered() {}

            @Override
            public void onGcPressureHigh() {
               pressureHighCallbacks.incrementAndGet();
            }

            @Override
            public void onGcPressureRelieved() {}
         }, Runnable::run);

         // Establish a clean baseline once startup GC activity has settled, so counts recorded
         // before both listeners were fully attached don't skew the comparison.
         waitUntilQuiescent(lastNotificationNanos);
         long baselineRecorded = monitor.getGcGeneration();
         long baselineKept = keptCount(actionStats);

         // Drive real garbage collection: churn short-lived allocations while keeping a small live
         // set, plus a few explicit collections to guarantee at least one full concurrent cycle.
         driveGarbageCollection();

         long recordedDelta;
         long keptDelta;
         while (true) {
            waitUntilQuiescent(lastNotificationNanos);
            long quietMark = lastNotificationNanos.get();
            keptDelta = keptCount(actionStats) - baselineKept;
            recordedDelta = monitor.getGcGeneration() - baselineRecorded;
            // If a stray notification slipped in between the two reads, re-quiesce and retry.
            if (lastNotificationNanos.get() == quietMark) {
               break;
            }
         }

         long cycleCount = actionStats.getOrDefault(CONCURRENT_CYCLE_ACTION, new long[]{0, 0})[0];
         long cycleMaxDur = actionStats.getOrDefault(CONCURRENT_CYCLE_ACTION, new long[]{0, 0})[1];
         boolean noGcObserved = totalCount(actionStats) == 0;

         StringBuilder actionsDump = new StringBuilder();
         for (Map.Entry<String, long[]> e : new TreeMap<>(actionStats).entrySet()) {
            actionsDump.append(e.getKey()).append("(n=").append(e.getValue()[0])
                  .append(",maxDurMs=").append(e.getValue()[1]).append(") ");
         }

         boolean pass = recordedDelta == keptDelta
               && pressureHighCallbacks.get() == 0
               && !monitor.isGcPressureExceeded()
               && !noGcObserved;

         System.out.println("GC_ACTIONS=" + actionsDump.toString().trim());
         System.out.println("NO_GC_OBSERVED=" + noGcObserved);
         System.out.println("CONCURRENT_CYCLE_OBSERVED=" + (cycleCount > 0));
         System.out.println("CONCURRENT_CYCLE_COUNT=" + cycleCount);
         System.out.println("CONCURRENT_CYCLE_MAX_DUR_MS=" + cycleMaxDur);
         System.out.println("MONITOR_RECORDED_DELTA=" + recordedDelta);
         System.out.println("KEPT_NOTIFICATION_DELTA=" + keptDelta);
         System.out.println("GC_PRESSURE_HIGH_CALLBACKS=" + pressureHighCallbacks.get());
         System.out.println("MONITOR_GC_PRESSURE=" + monitor.isGcPressureExceeded());
         System.out.println("RESULT=" + (pass ? "PASS" : "FAIL"));

         monitor.stop();
         for (NotificationEmitter emitter : emitters) {
            try {
               emitter.removeNotificationListener(raw);
            } catch (Exception ignore) {
               // best effort
            }
         }
         System.exit(pass ? 0 : 1);
      }

      private static long keptCount(Map<String, long[]> actionStats) {
         // Oracle is intentionally independent of MemoryMonitor's own predicate: the monitor must
         // record every notification whose action is not the concurrent-cycle action, and nothing else.
         long kept = 0;
         for (Map.Entry<String, long[]> e : actionStats.entrySet()) {
            if (!CONCURRENT_CYCLE_ACTION.equals(e.getKey())) {
               kept += e.getValue()[0];
            }
         }
         return kept;
      }

      private static long totalCount(Map<String, long[]> actionStats) {
         long total = 0;
         for (long[] v : actionStats.values()) {
            total += v[0];
         }
         return total;
      }

      private static void waitUntilQuiescent(AtomicLong lastNotificationNanos) throws InterruptedException {
         long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
         while (System.nanoTime() < deadline) {
            long sinceLast = System.nanoTime() - lastNotificationNanos.get();
            if (sinceLast > TimeUnit.MILLISECONDS.toNanos(QUIET_MS)) {
               return;
            }
            Thread.sleep(100);
         }
      }

      private static void driveGarbageCollection() throws InterruptedException {
         byte[][] live = new byte[16][];
         for (int i = 0; i < 4000; i++) {
            live[i % live.length] = new byte[256 * 1024];
            if (i % 500 == 0) {
               System.gc();
               Thread.sleep(20);
            }
         }
         // A couple of explicit full collections to guarantee a concurrent cycle on ZGC/Shenandoah.
         for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(100);
         }
      }
   }
}
