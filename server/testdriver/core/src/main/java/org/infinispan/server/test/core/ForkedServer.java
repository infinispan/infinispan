package org.infinispan.server.test.core;

import static org.infinispan.commons.test.Exceptions.unchecked;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.OS;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 11.0
 **/
public class ForkedServer {

   private static final Log log = org.infinispan.commons.logging.LogFactory.getLog(ForkedInfinispanServerDriver.class);

   private static final String START_PATTERN = "ISPN080001";

   private List<String> commands = new ArrayList<>();
   private Process process;
   private String serverHome, serverLogDir, serverLog;
   public static final int TIMEOUT_SECONDS = Integer.getInteger(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_FORKED_TIMEOUT_SECONDS, 30);
   public static final Integer DEFAULT_SINGLE_PORT = 11222;
   public static final int OFFSET_FACTOR = 100;

   public ForkedServer(String serverHome) {
      this.serverHome = serverHome;
      this.serverLogDir = serverHome + "/server/log";
      this.serverLog = serverLogDir + "/server.log";
      cleanServerLog();
      callInitScript();
   }

   private ForkedServer callInitScript() {
      String extension = OS.getCurrentOs() == OS.WINDOWS ? ".bat" : ".sh";
      commands.add(serverHome + String.format("/bin/server%s", extension));
      return this;
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
         Process process = Runtime.getRuntime().exec(String.format("tail -f %s", serverLog));
         try (Stream<String> lines = new BufferedReader(
               new InputStreamReader(process.getInputStream())).lines()) {
            return lines.peek(System.out::println).anyMatch(line -> line.contains(pattern));
         }
      });
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
      return Paths.get(serverHome + "/server/lib").toFile();
   }

   public long getPid() {
      return org.infinispan.commons.jdkspecific.ProcessInfo.of(process).getPid();
   }

}
