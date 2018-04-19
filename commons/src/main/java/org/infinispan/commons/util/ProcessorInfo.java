package org.infinispan.commons.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * Provides general information about the processors on this host.
 *
 * @author Jason T. Greene
 * @author Tristan Tarrant
 */
public class ProcessorInfo {
   private static final byte[] BITS = new byte[]{0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4};
   private static final String CPUS_ALLOWED = "Cpus_allowed:";
   private static final String CGROUP_STATUS_FILE = "/proc/cgroups";
   private static final String CPU_CFS_PERIOD_US = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";
   private static final String CPU_CFS_QUOTA_US = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
   private static final String CPU_SHARES = "/sys/fs/cgroup/cpu/cpu.shares";

   private ProcessorInfo() {
   }

   /**
    * Returns the number of processors available to this process. On most operating systems this method
    * simply delegates to {@link Runtime#availableProcessors()}. However, on Linux, this strategy
    * is insufficient, since the JVM does not take into consideration the process' CPU set affinity and the CGroups
    * quota/period assignment. Therefore this method will analyze the Linux proc filesystem
    * to make the determination. Since the CPU affinity of a process can be change at any time, this method does
    * not cache the result. Calls should be limited accordingly.
    * <br>
    * The number of available processors can be overridden via the system property <tt>infinispan.activeprocessorcount</tt>,
    * e.g. <tt>java -Dinfinispan.activeprocessorcount=4 ...</tt>. Note that this value cannot exceed the actual number
    * of available processors.
    * <br>
    * Since Java 10, this can also be achieved via the VM flag <tt>-XX:ActiveProcessorCount=xx</tt>.
    * <br>
    * Note that on Linux, both SMT units (Hyper-Threading) and CPU cores are counted as a processor.
    *
    * @return the available processors on this system.
    */
   public static int availableProcessors() {
      if (System.getSecurityManager() != null) {
         return AccessController.doPrivileged((PrivilegedAction<Integer>) () -> Integer.valueOf(determineProcessors())).intValue();
      }

      return determineProcessors();
   }

   private static int determineProcessors() {
      int javaProcs = Runtime.getRuntime().availableProcessors();
      int userProcs = Integer.getInteger("infinispan.activeprocessorcount", javaProcs);
      if (!isLinux()) {
         return userProcs;
      }

      // Obtain the CPU Mask
      int maskProcs = readCPUMask();
      if (maskProcs < 0)
         maskProcs = userProcs;

      // Obtain the CPU shares
      int cpuShares = readCPUShares();
      if (cpuShares == 1024)
         cpuShares = userProcs;

      // Obtain the CFS quota/period ratio
      long cpuQuota = readCPUCFSQuotaUS();
      double cpuRatio = cpuQuota < 0 ? userProcs : Math.ceil((float) cpuQuota / (float) readCPUCFSPeriodUS());

      return Math.min(userProcs, Math.min(maskProcs, Math.min(cpuShares, (int) cpuRatio)));
   }

   private static int readCPUMask() {
      try (Stream<String> lines = Files.lines(Paths.get("/proc/self/status"), StandardCharsets.US_ASCII)) {
         return lines.filter(line -> line.startsWith(CPUS_ALLOWED))
               .findFirst().map(line -> {
            int count = 0;
            int start = CPUS_ALLOWED.length();
            for (int i = start; i < line.length(); i++) {
               char ch = line.charAt(i);
               if (ch >= '0' && ch <= '9') {
                  count += BITS[ch - '0'];
               } else if (ch >= 'a' && ch <= 'f') {
                  count += BITS[ch - 'a' + 10];
               } else if (ch >= 'A' && ch <= 'F') {
                  count += BITS[ch - 'A' + 10];
               }
            }
            return count;
         }).orElse(-1);
      } catch (IOException e) {
         return -1;
      }
   }


   private static long readCPUCFSQuotaUS() {
      if (!hasCGroups())
         return -1l;
      try (Stream<String> lines = Files.lines(Paths.get(CPU_CFS_QUOTA_US), StandardCharsets.US_ASCII)) {
         return lines.findFirst()
               .map(line -> Long.parseLong(line)).orElse(-1l);
      } catch (IOException e) {
         return -1l;
      }
   }

   private static long readCPUCFSPeriodUS() {
      try (Stream<String> lines = Files.lines(Paths.get(CPU_CFS_PERIOD_US), StandardCharsets.US_ASCII)) {
         return lines.findFirst()
               .map(line -> Long.parseLong(line)).orElse(100_000l);
      } catch (IOException e) {
         return 100_000l;
      }
   }

   private static int readCPUShares() {
      try (Stream<String> lines = Files.lines(Paths.get(CPU_SHARES), StandardCharsets.US_ASCII)) {
         return lines.findFirst()
               .map(line -> Integer.parseInt(line)).orElse(1024);
      } catch (IOException e) {
         return 1024;
      }
   }

   private static boolean isLinux() {
      String osArch = System.getProperty("os.name", "unknown").toLowerCase(Locale.US);
      return (osArch.contains("linux"));
   }

   private static boolean hasCGroups() {
      return Files.exists(Paths.get(CGROUP_STATUS_FILE));
   }

   public static void main(String args[]) {
      System.out.println(determineProcessors());
   }
}
