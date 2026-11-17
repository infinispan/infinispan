package org.infinispan.test.fwk;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes which processes hold UDP ports, so a test cluster that fails to bind its transport can be explained
 * afterwards.
 * <p>
 * A test cluster fails to start with a {@link java.net.BindException} when another socket holds its multicast port
 * and doesn't allow the port to be shared. That happens rarely and can't be reproduced on demand, so the evidence has
 * to be collected when it happens. The description lists every UDP socket bound to a port in a range, the process
 * owning it, and whether that process is the test JVM itself.
 * </p>
 * <p>
 * The information comes from the Linux {@code /proc} file system. On other systems the description says it is not
 * available. Sockets of processes the test user can't inspect are listed without an owner.
 * </p>
 */
final class UdpPortOwners {
   private static final Path PROC = Path.of("/proc");
   private static final Path[] UDP_TABLES = {PROC.resolve("net/udp"), PROC.resolve("net/udp6")};

   private UdpPortOwners() { }

   /**
    * Describes the UDP sockets bound to a port in a range, and the processes that own them.
    * <p>
    * Each socket is one line with its local address and port, its socket inode, and its owner: the process id and
    * executable, marked when the owner is this JVM. The method never throws: anything that can't be read is part of
    * the description instead.
    * </p>
    *
    * @param fromPort the first port of the range, inclusive
    * @param toPort   the last port of the range, inclusive
    * @return a multi-line description of the sockets in the range, a line saying that none is bound, or a line saying
    *         why the information is not available
    */
   static String describe(int fromPort, int toPort) {
      try {
         return describeSockets(fromPort, toPort);
      } catch (RuntimeException e) {
         // The caller logs this while handling a bind failure, so a surprise here must never replace that failure
         return "UDP socket owners not available: " + e;
      }
   }

   /**
    * Describes the UDP sockets bound to a port in a range, and the processes that own them, letting unexpected
    * failures escape.
    *
    * @param fromPort the first port of the range, inclusive
    * @param toPort   the last port of the range, inclusive
    * @return the description returned by {@link #describe(int, int)}
    * @throws RuntimeException if {@code /proc} has content or access rules this class doesn't expect
    */
   private static String describeSockets(int fromPort, int toPort) {
      if (!Files.isReadable(UDP_TABLES[0])) {
         return "UDP socket owners not available: " + UDP_TABLES[0] + " can't be read";
      }
      // inode -> socket description, in the order the kernel lists them
      Map<String, String> sockets = new HashMap<>();
      List<String> inodes = new ArrayList<>();
      StringBuilder errors = new StringBuilder();
      for (Path table : UDP_TABLES) {
         try {
            for (String line : Files.readAllLines(table)) {
               addSocketInRange(line, fromPort, toPort, sockets, inodes);
            }
         } catch (IOException e) {
            errors.append(System.lineSeparator()).append("Could not read ").append(table).append(": ").append(e);
         }
      }
      if (inodes.isEmpty()) {
         return "No UDP socket bound to a port in " + fromPort + "-" + toPort + errors;
      }
      Map<String, Long> owners = findOwners(inodes);
      long self = ProcessHandle.current().pid();
      StringBuilder description = new StringBuilder();
      description.append("UDP sockets bound to a port in ").append(fromPort).append('-').append(toPort).append(':');
      for (String inode : inodes) {
         description.append(System.lineSeparator()).append("  ").append(sockets.get(inode)).append(", owner: ");
         Long pid = owners.get(inode);
         if (pid == null) {
            description.append("unknown (process not readable by this user)");
         } else {
            description.append("pid ").append(pid);
            if (pid == self) {
               description.append(" (this JVM)");
            }
            description.append(' ').append(executable(pid));
         }
      }
      return description.append(errors).toString();
   }

   /**
    * Adds the socket of one {@code /proc/net/udp} line if its local port is in the range.
    *
    * @param line     a line of the kernel's UDP socket table; the header line is ignored
    * @param fromPort the first port of the range, inclusive
    * @param toPort   the last port of the range, inclusive
    * @param sockets  receives the socket's description, keyed by its inode
    * @param inodes   receives the socket's inode, in table order
    */
   private static void addSocketInRange(String line, int fromPort, int toPort, Map<String, String> sockets,
                                        List<String> inodes) {
      // Columns: sl local_address rem_address st tx_queue:rx_queue tr:tm->when retrnsmt uid timeout inode ...
      String[] columns = line.trim().split("\\s+");
      if (columns.length < 10 || !columns[0].endsWith(":")) {
         return;
      }
      String localAddress = columns[1];
      int separator = localAddress.lastIndexOf(':');
      int port = Integer.parseInt(localAddress.substring(separator + 1), 16);
      String inode = columns[9];
      if (port < fromPort || port > toPort || sockets.containsKey(inode)) {
         return;
      }
      sockets.put(inode, String.format("%s:%d (inode %s, uid %s)",
            ipAddress(localAddress.substring(0, separator)), port, inode, columns[7]));
      inodes.add(inode);
   }

   /**
    * Converts an address from the kernel's socket table to a readable form.
    *
    * @param hex the address as the kernel lists it: 8 hexadecimal digits in host byte order for IPv4, 32 for IPv6
    * @return the IPv4 address in dotted form, or the IPv6 address as listed by the kernel
    */
   private static String ipAddress(String hex) {
      if (hex.length() != 8) {
         return "[" + hex + "]";
      }
      // The kernel prints the 32-bit address in host byte order, so on little-endian hosts the bytes are reversed
      int raw = (int) Long.parseLong(hex, 16);
      int address = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? Integer.reverseBytes(raw) : raw;
      return (address >>> 24) + "." + ((address >>> 16) & 0xFF) + "." + ((address >>> 8) & 0xFF) + "." + (address & 0xFF);
   }

   /**
    * Finds the processes owning sockets, among the processes this user can inspect.
    *
    * @param inodes the socket inodes to look for
    * @return the process id owning each socket that was found, keyed by inode; sockets not found are absent
    */
   private static Map<String, Long> findOwners(List<String> inodes) {
      Map<String, String> links = new HashMap<>();
      for (String inode : inodes) {
         links.put("socket:[" + inode + "]", inode);
      }
      Map<String, Long> owners = new HashMap<>();
      try (DirectoryStream<Path> processes = Files.newDirectoryStream(PROC, "[0-9]*")) {
         for (Path process : processes) {
            long pid = Long.parseLong(process.getFileName().toString());
            try (DirectoryStream<Path> descriptors = Files.newDirectoryStream(process.resolve("fd"))) {
               for (Path descriptor : descriptors) {
                  String inode;
                  try {
                     inode = links.get(Files.readSymbolicLink(descriptor).toString());
                  } catch (IOException e) {
                     // The descriptor was closed while scanning, which is frequent in a busy JVM
                     continue;
                  }
                  if (inode != null) {
                     owners.putIfAbsent(inode, pid);
                  }
               }
            } catch (IOException e) {
               // Another user's process, or one that exited while scanning
            }
         }
      } catch (IOException e) {
         // Leaves the remaining sockets without an owner
      }
      return owners;
   }

   /**
    * Returns the executable a process runs, without its arguments.
    * <p>
    * Arguments are left out because they can hold credentials, and the description ends up in test logs.
    * </p>
    *
    * @param pid the process id
    * @return the path of the process's executable, or a note saying it isn't known
    */
   private static String executable(long pid) {
      return ProcessHandle.of(pid)
            .flatMap(process -> process.info().command())
            .orElse("(command unknown)");
   }
}
