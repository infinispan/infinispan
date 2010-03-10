/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2010, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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

package org.infinispan.server.core;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import org.infinispan.Version;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 */
public class Main {

   private static final Log log = LogFactory.getLog(Main.class);
   public static final String PROP_KEY_PORT = "infinispan.server.port";
   public static final String PROP_KEY_HOST = "infinispan.server.host";
   public static final String PROP_KEY_MASTER_THREADS = "infinispan.server.master.threads";
   public static final String PROP_KEY_WORKER_THREADS = "infinispan.server.worker.threads";
   public static final String PROP_KEY_CACHE_CONFIG = "infinispan.server.cache.config";
   public static final String PROP_KEY_PROTOCOL = "infinispan.server.protocol";

   public static final int PORT_DEFAULT = 11211;
   public static final String HOST_DEFAULT = "127.0.0.1";
   public static final int MASTER_THREADS_DEFAULT = 0;
   public static final int WORKER_THREADS_DEFAULT = 0;

   private static String programName;

   /**
    * Server properties.  This object holds all of the required
    * information to get the server up and running. Use System
    * properties for defaults.
    */
   private Map<String, String> props = new HashMap<String, String>();

   private ProtocolServer server;

   /**
    * Explicit constructor.
    */
   public Main() {
      // Set default properties
      Properties sysProps = System.getProperties();
      for (Object propName : sysProps.keySet()) {
         String propNameString = (String) propName;
         String propValue = (String) sysProps.get(propNameString);
         props.put(propNameString, propValue);
      }
   }

   public void boot(String[] args) throws Exception {
      // First process the command line to pickup custom props/settings
      processCommandLine(args);

      int port = props.get(PROP_KEY_PORT) == null ? PORT_DEFAULT : Integer.parseInt(props.get(PROP_KEY_PORT));
      String host = props.get(PROP_KEY_HOST) == null ? HOST_DEFAULT : props.get(PROP_KEY_HOST);
      int masterThreads = props.get(PROP_KEY_MASTER_THREADS) == null ? MASTER_THREADS_DEFAULT : Integer.parseInt(props.get(PROP_KEY_MASTER_THREADS));
      if (masterThreads < 0) {
         throw new IllegalArgumentException("Master threads can't be lower than 0: " + masterThreads);
      }
      int workerThreads = props.get(PROP_KEY_WORKER_THREADS) == null ? WORKER_THREADS_DEFAULT : Integer.parseInt(props.get(PROP_KEY_WORKER_THREADS));
      if (workerThreads < 0) {
         throw new IllegalArgumentException("Worker threads can't be lower than 0: " + masterThreads);
      }
      String configFile = props.get(PROP_KEY_CACHE_CONFIG);

      String protocol = props.get(PROP_KEY_PROTOCOL);
      if (protocol == null) {
         System.err.println("ERROR: Please indicate protocol to run with -r parameter");
         showAndExit();
      }
      Protocol p = Protocol.valueOf(protocol.toUpperCase());
      server = (ProtocolServer) Util.getInstance(p.clazz);
      addShutdownHook(new ShutdownHook(server)); // Make a shutdown hook
      server.start(host, port, configFile, masterThreads, workerThreads);
   }

   private void processCommandLine(String[] args) throws Exception {
      // set this from a system property or default to jboss
      programName = System.getProperty("program.name", "startServer");
      // TODO: Since this memcached/hotrod implementation stores stuff as byte[], these could be implemented in the future:
      // -m <num>      max memory to use for items in megabytes (default: 64 MB)
      // -M            return error on memory exhausted (rather than removing items)
      String sopts = "-:hD:Vp:l:m:t:c:r:";
      LongOpt[] lopts =
      {new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h'),
            new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V'),
            new LongOpt("port", LongOpt.REQUIRED_ARGUMENT, null, 'p'),
            new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'l'),
            new LongOpt("master_threads", LongOpt.REQUIRED_ARGUMENT, null, 'm'),
            new LongOpt("worker_threads", LongOpt.REQUIRED_ARGUMENT, null, 't'),
            new LongOpt("cache_config", LongOpt.REQUIRED_ARGUMENT, null, 'c'),
            new LongOpt("protocol", LongOpt.REQUIRED_ARGUMENT, null, 'r'),};
      Getopt getopt = new Getopt(programName, args, sopts, lopts);

      int code;
      while ((code = getopt.getopt()) != -1)
      {
         switch (code)
         {
            case ':' :
            case '?' :
               // for now both of these should exit with error status
               System.exit(1);
               break; // for completeness
            case 1 :
               // this will catch non-option arguments
               // (which we don't currently care about)
               System.err.println(programName + ": unused non-option argument: " + getopt.getOptarg());
               break; // for completeness
            case 'h' :
               // show command line help
               showAndExit();
               break; // for completeness
            case 'V' :
               Version.printFullVersionInformation();
               System.exit(0);
               break;
            case 'p' :
               props.put(PROP_KEY_PORT, getopt.getOptarg());
               break;
            case 'l' :
               props.put(PROP_KEY_HOST, getopt.getOptarg());
               break;
            case 'm' :
               props.put(PROP_KEY_MASTER_THREADS, getopt.getOptarg());
               break;
            case 't' :
               props.put(PROP_KEY_WORKER_THREADS, getopt.getOptarg());
               break;
            case 'c' :
               props.put(PROP_KEY_CACHE_CONFIG, getopt.getOptarg());
               break;
            case 'r' :
               props.put(PROP_KEY_PROTOCOL, getopt.getOptarg());
               break;
            case 'D' :
               // set a system property
               String arg = getopt.getOptarg();
               String name, value;
               int i = arg.indexOf("=");
               if (i == -1) {
                  name = arg;
                  value = "true";
               } else {
                  name = arg.substring(0, i);
                  value = arg.substring(i + 1, arg.length());
               }
               System.setProperty(name, value);
               break;
            default :
               // this should not happen,
               // if it does throw an error so we know about it
               throw new Error("unhandled option code: " + code);
         }
      }
   }

   public static void main(final String[] args) throws Exception {
      log.info("Start main with args: {0}", Arrays.toString(args));
      Callable<Void> worker = new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            try {
               Main main = new Main();
               main.boot(args);
            } catch (Exception e) {
               System.err.println("Failed to boot JBoss:");
               e.printStackTrace();
               throw e;
            }
            return null;
         }
      };

      Future<Void> f = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r, "InfinispanServer-Main");
         }
      }).submit(worker);

      f.get();
   }

   /**
    * Adds the specified shutdown hook
    *
    * @param shutdownHook
    */
   private static void addShutdownHook(final Thread shutdownHook) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
         public Void run() {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            return null;
         }
      });
   }

   private static void showAndExit() {
      System.out.println("usage: " + programName + " [options]");
      System.out.println();
      System.out.println("options:");
      System.out.println("    -h, --help                         Show this help message");
      System.out.println("    -V, --version                      Show version information");
      System.out.println("    --                                 Stop processing options");
      System.out.println("    -p, --port=<num>                   TCP port number to listen on (default: 11211)");
      System.out.println("    -l, --host=<host or ip>            Interface to listen on (default: 127.0.0.1, localhost)");
      System.out.println("    -m, --master_threads=<num>         Number of threads accepting incoming connections (default: unlimited while resources are available)");
      System.out.println("    -t, --work_threads=<num>           Number of threads processing incoming requests and sending responses (default: unlimited while resources are available)");
      System.out.println("    -c, --cache_config=<filename>      Cache configuration file (default: creates cache with default values)");
      System.out.println("    -r, --protocol=[memcached|hotrod]  Protocol to understand by the server. This is a mandatory option and you should choose one of the two options");
      System.out.println("    -D<name>[=<value>]                 Set a system property");
      System.out.println();
      System.exit(0);
   }

   /**
    * ShutdownHook
    */
   private static class ShutdownHook extends Thread {
      private final ProtocolServer server;

      ShutdownHook(ProtocolServer server) {
         this.server = server;
      }

      @Override
      public void run() {
         // If we have a server
         if (server != null) {
            // Log out
            System.out.println("Posting Shutdown Request to the server...");
            // Start in new thread to give positive feedback to requesting client of success.
            Future<Void> f = Executors.newSingleThreadExecutor().submit(new Callable<Void>() {
               @Override
               public Void call() throws Exception {
                  server.stop();
                  return null;
               }
            });

            // Block until done
            try {
               f.get();
            } catch (InterruptedException ie) {
               // Clear the flag
               Thread.interrupted();
            } catch (ExecutionException e) {
               throw new RuntimeException("Exception encountered in shutting down the server", e);
            }
         }
      }
   }
   
}
