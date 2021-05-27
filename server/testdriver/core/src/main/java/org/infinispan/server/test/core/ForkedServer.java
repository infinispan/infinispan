package org.infinispan.server.test.core;

import static org.infinispan.commons.test.Exceptions.unchecked;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.OS;

/**
 * Forked server starts the server using batch scripts from the Infinispan Server distribution to start the server.
 * The driver uses REST API to shutdown the server.
 * Server log is cleaned before start and is not configurable.
 * Appends random UUID for server process to identify the corresponding child Java process of the server.
 *
 * FIXME: The server.log file must be present before starting the server as the monitoring process is pointed to it -
 *        - groundwork for making the purging of the log directory optional.
 *
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @author Radoslav Husar
 * @since 11.0
 **/
public class ForkedServer {

   private static final Log log = LogFactory.getLog(ForkedInfinispanServerDriver.class);
   private static final String START_PATTERN = "ISPN080001";

   // Static driver configuration
   public static final int TIMEOUT_SECONDS = Integer.getInteger(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_FORKED_TIMEOUT_SECONDS, 30);
   public static final Integer DEFAULT_SINGLE_PORT = 11222;
   public static final int OFFSET_FACTOR = 100;

   // Dynamic server configuration
   private final List<String> commands = new ArrayList<>();
   private final String serverHome;
   private final String serverLogDir;
   private final String serverLog;
   private String jvmOptions;
   private String serverConfiguration;

   // Runtime
   private final UUID serverId;
   private Process process;
   private Thread logMonitor;
   private final CountDownLatch isServerStarted = new CountDownLatch(1);

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
      addVmArgument(this.getClass().getName() + "-pid", this.serverId);
   }

   public ForkedServer setServerConfiguration(String serverConfiguration) {
      commands.add("-c");
      if (!new File(serverConfiguration).isAbsolute()) {
         serverConfiguration = getClass().getClassLoader().getResource(serverConfiguration).getPath();
      }
      this.serverConfiguration = serverConfiguration;
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

   public ForkedServer setJvmOptions(String jvmOptions) {
      this.jvmOptions = jvmOptions;
      return this;
   }

   public ForkedServer start() {
      ProcessBuilder pb = new ProcessBuilder();
      pb.command(commands);

      if (jvmOptions != null) {
         pb.environment().put("JAVA_OPTS", jvmOptions);
      }

      pb.redirectErrorStream(true);
      try {
         process = pb.start();

         // Start a server monitoring thread which
         // (1) prints out the server log to the test output, and
         // (2) monitors whether server has started.
         logMonitor = new Thread(getServerMonitorRunnable(process.getInputStream()));
         logMonitor.start();

         // Await server start
         // FIXME The waiting should really be done in ForkedInfinispanServerDriver#start to support concurrent boot
         if (!isServerStarted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException(String.format("The server couldn't start within %d seconds!", TIMEOUT_SECONDS));
         }
      } catch (Exception e) {
         log.error(e);
      }
      return this;
   }

   public Runnable getServerMonitorRunnable(InputStream outputStream) {
      return () -> {
         try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(outputStream));
            String line;
            while ((line = reader.readLine()) != null) {
               log.info(line);
               if (line.contains(START_PATTERN)) {
                  isServerStarted.countDown();
               }
            }
            reader.close();
         } catch (IOException ex) {
            log.error(ex);
         }
      };
   }

   public void stopInternal() {
      try {
         process.destroy();
      } catch (Exception ex) {
         log.error(ex);
      }

      try {
         logMonitor.interrupt();
      } catch (Exception ex) {
         log.error(ex);
      }
   }

   private void cleanServerLog() {
      unchecked(() -> {
         Files.deleteIfExists(Paths.get(serverLog));
         boolean isServerLogDirectoryExist = Files.exists(Paths.get(serverLogDir));
         if (!isServerLogDirectoryExist) {
            Files.createDirectory(Paths.get(serverLogDir));
         }
         Files.createFile(Paths.get(serverLog));
      });
   }

   public File getServerLib() {
      return Paths.get(serverHome + File.separator + "server" + File.separator + "lib").toFile();
   }

   /**
    * Returns the process ID (PID) of the Java process as opposed to the PID of the parent shell script which started
    * the actual Java process. The process is matched containing the randomized UUID which was passed on server start.
    * This employs {@code jps} utility to list Java processes and thus requires the executable to be in the system path.
    *
    * @return process ID (pid) of the server's Java process.
    * @throws IllegalStateException if PID cannot be determined; e.g. because the server is no longer running or jps utility is not available
    */
   public long getPid() throws IllegalStateException {
      try {
         Process jpsProcess = Runtime.getRuntime().exec(String.format("jps%s -v", (OS.getCurrentOs() == OS.WINDOWS) ? ".exe" : ""));
         BufferedReader input = new BufferedReader(new InputStreamReader(jpsProcess.getInputStream()));
         String jpsLine;
         while ((jpsLine = input.readLine()) != null) {
            if (jpsLine.contains(this.serverId.toString())) {
               jpsProcess.destroyForcibly();
               long pid = Long.parseLong(jpsLine.trim().split("\\s+")[0]);
               log.infof("Obtained pid is %d for process with UUID %s.", pid, this.serverId);
               return pid;
            }
         }
         input.close();
         jpsProcess.destroyForcibly();
      } catch (Exception ex) {
         throw new IllegalStateException(ex);
      }

      throw new IllegalStateException("Unable to determine PID of the running Infinispan server.");
   }

   public String getServerConfiguration() {
      return serverConfiguration;
   }

   public void addVmArgument(String key, Object value) {
      commands.add(String.format("-D%s=%s", key, value));
   }
}
