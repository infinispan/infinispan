package org.infinispan.server.core

import scala.collection.JavaConversions._
import collection.mutable.HashMap
import scala.collection.mutable
import org.infinispan.util.Util
import java.io.IOException
import java.security.{PrivilegedAction, AccessController}
import java.util.concurrent.{ThreadFactory, ExecutionException, Callable, Executors}
import gnu.getopt.{Getopt, LongOpt}
import org.infinispan.Version
import org.infinispan.manager.{CacheManager, DefaultCacheManager}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
object Main extends Logging {
   
   val PROP_KEY_PORT = "infinispan.server.port"
   val PROP_KEY_HOST = "infinispan.server.host"
   val PROP_KEY_MASTER_THREADS = "infinispan.server.master.threads"
   val PROP_KEY_WORKER_THREADS = "infinispan.server.worker.threads"
   val PROP_KEY_CACHE_CONFIG = "infinispan.server.cache.config"
   val PROP_KEY_PROTOCOL = "infinispan.server.protocol"
   val PROP_KEY_IDLE_TIMEOUT = "infinispan.server.idle.timeout"
   val PORT_DEFAULT = 11211
   val HOST_DEFAULT = "127.0.0.1"
   val MASTER_THREADS_DEFAULT = 0
   val WORKER_THREADS_DEFAULT = 0
   val IDLE_TIMEOUT_DEFAULT = 60

   /**
    * Server properties.  This object holds all of the required
    * information to get the server up and running. Use System
    * properties for defaults.
    */
   private val props: mutable.Map[String, String] = {
      // Set default properties
      val properties = new HashMap[String, String]
      val sysProps = System.getProperties
      for (property <- asIterator(sysProps.iterator))
         properties.put(property._1, property._2)
      properties
   }
   
   private var programName: String = _
   private var server: ProtocolServer = _

   def main(args: Array[String]) {
      info("Start main with args: {0}", args.mkString(", "))
      var worker = new Callable[Void] {
         override def call = {
            try {
               boot(args)
            }
            catch {
               case e: Exception => {
                  System.err.println("Failed to boot JBoss:")
                  e.printStackTrace
                  throw e
               }
            }
            null
         }
      }
      var f = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
         override def newThread(r: Runnable): Thread = {
            // TODO Maybe create thread names based on the protocol run
            return new Thread(r, "InfinispanServer-Main")
         }
      }).submit(worker)
      f.get
   }

   def boot(args: Array[String]) {
      // First process the command line to pickup custom props/settings
      processCommandLine(args)

      val host = if (props.get(PROP_KEY_HOST) == None) HOST_DEFAULT else props.get(PROP_KEY_HOST).get
      val masterThreads = if (props.get(PROP_KEY_MASTER_THREADS) == None) MASTER_THREADS_DEFAULT else props.get(PROP_KEY_MASTER_THREADS).get.toInt
      if (masterThreads < 0)
         throw new IllegalArgumentException("Master threads can't be lower than 0: " + masterThreads)

      val workerThreads = if (props.get(PROP_KEY_WORKER_THREADS) == None) WORKER_THREADS_DEFAULT else props.get(PROP_KEY_WORKER_THREADS).get.toInt
      if (workerThreads < 0)
         throw new IllegalArgumentException("Worker threads can't be lower than 0: " + masterThreads)

      val configFile = props.get(PROP_KEY_CACHE_CONFIG)
      var protocol = props.get(PROP_KEY_PROTOCOL)
      if (protocol == None) {
         System.err.println("ERROR: Please indicate protocol to run with -r parameter")
         showAndExit
      }
      val idleTimeout = if (props.get(PROP_KEY_IDLE_TIMEOUT) == None) IDLE_TIMEOUT_DEFAULT else props.get(PROP_KEY_IDLE_TIMEOUT).get.toInt
      if (idleTimeout < 0)
         throw new IllegalArgumentException("Idle timeout can't be lower than 0: " + idleTimeout)

      // TODO: move class name and protocol number to a resource file under the corresponding project
      val clazz = protocol.get match {
         case "memcached" => "org.infinispan.server.memcached.MemcachedServer"
         case "hotrod" => "org.infinispan.server.hotrod.HotRodServer"
      }
      val port = {
         if (props.get(PROP_KEY_PORT) == None) {
            protocol.get match {
               case "memcached" => 11211
               case "hotrod" => 11311
            }
         } else {
            props.get(PROP_KEY_PORT).get.toInt
         }
      }

      var server = Util.getInstance(clazz).asInstanceOf[ProtocolServer]
      val cacheManager = if (configFile == None) new DefaultCacheManager else new DefaultCacheManager(configFile.get)
      addShutdownHook(new ShutdownHook(server, cacheManager))
      server.start(host, port, cacheManager, masterThreads, workerThreads, idleTimeout)
   }

   private def processCommandLine(args: Array[String]) {
      programName = System.getProperty("program.name", "startServer")
      var sopts = "-:hD:Vp:l:m:t:c:r:"
      var lopts = Array(
         new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h'),
         new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V'),
         new LongOpt("port", LongOpt.REQUIRED_ARGUMENT, null, 'p'),
         new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'l'),
         new LongOpt("master_threads", LongOpt.REQUIRED_ARGUMENT, null, 'm'),
         new LongOpt("worker_threads", LongOpt.REQUIRED_ARGUMENT, null, 't'),
         new LongOpt("cache_config", LongOpt.REQUIRED_ARGUMENT, null, 'c'),
         new LongOpt("protocol", LongOpt.REQUIRED_ARGUMENT, null, 'r'),
         new LongOpt("idle_timeout", LongOpt.REQUIRED_ARGUMENT, null, 'i'))
      var getopt = new Getopt(programName, args, sopts, lopts)
      var code: Int = 0
      while ((({code = getopt.getopt; code})) != -1) {
         code match {
            case ':' | '?' => System.exit(1)
            case 1 => System.err.println(programName + ": unused non-option argument: " + getopt.getOptarg)
            case 'h' => showAndExit
            case 'V' => {
               Version.printFullVersionInformation
               System.exit(0)
            }
            case 'p' => props.put(PROP_KEY_PORT, getopt.getOptarg)
            case 'l' => props.put(PROP_KEY_HOST, getopt.getOptarg)
            case 'm' => props.put(PROP_KEY_MASTER_THREADS, getopt.getOptarg)
            case 't' => props.put(PROP_KEY_WORKER_THREADS, getopt.getOptarg)
            case 'c' => props.put(PROP_KEY_CACHE_CONFIG, getopt.getOptarg)
            case 'r' => props.put(PROP_KEY_PROTOCOL, getopt.getOptarg)
            case 'i' => props.put(PROP_KEY_IDLE_TIMEOUT, getopt.getOptarg)
            case 'D' => {
               val arg = getopt.getOptarg
               var name = ""
               var value = ""
               var i = arg.indexOf("=")
               if (i == -1) {
                  name = arg
                  value = "true"
               } else {
                  name = arg.substring(0, i)
                  value = arg.substring(i + 1, arg.length)
               }
               System.setProperty(name, value)
            }
            case _ => throw new Exception("unhandled option code: " + code)
         }
      }
   }

   private def addShutdownHook(shutdownHook: Thread): Unit = {
      AccessController.doPrivileged(new PrivilegedAction[Void] {
         override def run = {
            Runtime.getRuntime.addShutdownHook(shutdownHook)
            null
         }
      })
   }

   private def showAndExit {
      println("usage: " + programName + " [options]")
      println
      println("options:")
      println("    -h, --help                         Show this help message")
      println
      println("    -V, --version                      Show version information")
      println
      println("    --                                 Stop processing options")
      println
      println("    -p, --port=<num>                   TCP port number to listen on (default: 11211 for Memcached servers, 11311 for Hot Rod servers)")
      println
      println("    -l, --host=<host or ip>            Interface to listen on (default: 127.0.0.1, localhost)")
      println
      println("    -m, --master_threads=<num>         Number of threads accepting incoming connections (default: unlimited while resources are available)")
      println
      println("    -t, --work_threads=<num>           Number of threads processing incoming requests and sending responses (default: unlimited while resources are available)")
      println
      println("    -c, --cache_config=<filename>      Cache configuration file (default: creates cache with default values)")
      println
      println("    -r, --protocol=[memcached|hotrod]  Protocol to understand by the server. This is a mandatory option and you should choose one of the two options")
      println
      println("    -i, --idle_timeout=<num>           Idle read timeout, in seconds, used to detect stale connections (default: 60 seconds).")
      println("                                       If no new messages have been read within this time, the server disconnects the channel.")
      println("                                       Passing 0 disables idle timeout.")
      println
      println("    -D<name>[=<value>]                 Set a system property")
      println
      System.exit(0)
   }
}

private class ShutdownHook(server: ProtocolServer, cacheManager: CacheManager) extends Thread {
   override def run {
      if (server != null) {
         System.out.println("Posting Shutdown Request to the server...")
         var f = Executors.newSingleThreadExecutor.submit(new Callable[Void] {
            override def call = {
               server.stop
               cacheManager.stop
               null
            }
         })
         try {
            f.get
         }
         catch {
            case ie: IOException => Thread.interrupted
            case e: ExecutionException => throw new RuntimeException("Exception encountered in shutting down the server", e)
         }
      }
   }
}
