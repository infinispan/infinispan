package org.infinispan.server.test.core;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.OS;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.infinispan.commons.test.Exceptions.unchecked;

/**
 * Forked server starts the server using batch scripts from the Infinispan Server distribution to start the server.
 * The driver uses REST API to shutdown the server.
 * Server log is cleaned before start and is not configurable.
 * Appends random UUID for server process to identify the corresponding child Java process of the server.
 *
 * FIXME: Add proper Windows platform support.
 * FIXME: The server.log file must be present before starting the server as the monitoring process is pointed to it -
 *        - groundwork for making the purging of the log directory optional.
 *
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @author Radoslav Husar
 * @since 11.0
 **/
public class ForkedServer {

   private static final Log log = org.infinispan.commons.logging.LogFactory.getLog(ForkedInfinispanServerDriver.class);

   private static final String START_PATTERN = "ISPN080001";

   private final List<String> commands = new ArrayList<>();
   private final UUID serverId;
   private Process process;
   private Process serverLogProcess;
   private final String serverHome;
   private final String serverLogDir;
   private final String serverLog;
   public static final int TIMEOUT_SECONDS = Integer.getInteger(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_FORKED_TIMEOUT_SECONDS, 30);
   public static final Integer DEFAULT_SINGLE_PORT = 11222;
   public static final int OFFSET_FACTOR = 100;

   public ForkedServer(String serverHome) {
      this.serverId = UUID.randomUUID();
      this.serverHome = serverHome;
      this.serverLogDir = serverHome + File.separator + "server" + File.separator + "log";
      this.serverLog = serverLogDir + File.separator + "server.log";
      cleanServerLog();
      callInitScript();
   }

   private void callInitScript() {
      String extension = OS.getCurrentOs() == OS.WINDOWS ? ".bat" : ".sh";
      commands.add(serverHome + File.separator + "bin" + File.separator + "server" + extension);

      // Append random UUID for this server to be used in the case of forceful shutdown.
      commands.add(String.format("-D%s-pid=%s", this.getClass().getName(), this.serverId));
   }

   public ForkedServer setServerConfiguration(String serverConfiguration) {
      commands.add("-c");
      if (!new File(serverConfiguration).isAbsolute()) {
         serverConfiguration = getClass().getClassLoader().getResource(serverConfiguration).getPath();
      }
      commands.add(serverConfiguration);
      return this;
   }

   public ForkedServer setPortsOffset(int numServer) {
      if (numServer >= 1) {
         commands.add("-o");
         commands.add(String.valueOf(OFFSET_FACTOR * numServer));
      }
      return this;
   }

   public ForkedServer start() {
      boolean isServerStarted;
      ProcessBuilder pb = new ProcessBuilder();
      pb.command(commands);
      try {
         process = pb.start();
         isServerStarted = runWithTimeout(this::checkServerLog, START_PATTERN);
         if (!isServerStarted) {
            throw new IllegalStateException("The server couldn't start");
         }
      } catch (Exception e) {
         log.error(e);
      }
      return this;
   }

   public boolean runWithTimeout(Function<String, Boolean> function, String logPattern) {
      ExecutorService executor = Executors.newSingleThreadExecutor();
      Callable<Boolean> task = () -> function.apply(logPattern);
      Future<Boolean> future = executor.submit(task);
      return unchecked(() -> future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
   }

   private boolean checkServerLog(String pattern) {
      return unchecked(() -> {
         serverLogProcess = Runtime.getRuntime().exec(String.format("tail -f %s", serverLog));
         try (Stream<String> lines = new BufferedReader(
               new InputStreamReader(process.getInputStream())).lines()) {
            return lines.peek(System.out::println).anyMatch(line -> line.contains(pattern));
         }
      });
   }

   public void cleanup() {
      this.serverLogProcess.destroy();
   }

   public void printServerLog(Consumer<String> c) {
      try (Stream<String> s = Files.lines(Paths.get(serverLog))) {
         s.forEach(c);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   private void cleanServerLog() {
      unchecked(() -> {
         Files.deleteIfExists(Paths.get(serverLog));
         boolean isServerLogDirectoryExist = Files.exists(Paths.get(serverLogDir));
         if (!isServerLogDirectoryExist)
            Files.createDirectory(Paths.get(serverLogDir));
         Files.createFile(Paths.get(serverLog));
      });
   }

   public File getServerLib() {
      return Paths.get(serverHome + File.separator + "server" + File.separator + "lib").toFile();
   }

   /**
    * Returns the process ID of the Java process as opposed to the pid of the shell script which started the actual
    * Java process. The process is matched containing the randomized UUID.
    *
    * @return process ID (pid) of the server's Java process.
    */
   public long getPid() {
      try {
         Process psProcess = Runtime.getRuntime().exec("ps");
         BufferedReader input = new BufferedReader(new InputStreamReader(psProcess.getInputStream()));
         String psLine;
         while ((psLine = input.readLine()) != null) {
            if (psLine.contains(this.serverId.toString()) && psLine.contains("bin" + File.separator + "java")) {
               psProcess.destroyForcibly();
               long pid = Long.parseLong(psLine.substring(0, psLine.indexOf(" ")));
               log.infof("Obtained pid is %d for process %s.", pid, this.serverId);
               return pid;
            }
         }
         input.close();
         process.destroy();
      } catch (Exception ex) {
         // Ignore.
      }

      throw new UnsupportedOperationException();
   }

}
