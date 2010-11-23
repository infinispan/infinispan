package org.infinispan.server.core

import java.io.IOException
import java.security.{PrivilegedAction, AccessController}
import java.util.concurrent.{ThreadFactory, ExecutionException, Callable, Executors}
import gnu.getopt.{Getopt, LongOpt}
import org.infinispan.Version
import java.util.Properties
import org.infinispan.util.{TypedProperties, Util}
import org.infinispan.config.{Configuration, GlobalConfiguration}
import org.infinispan.manager.{EmbeddedCacheManager, CacheContainer, DefaultCacheManager}
import org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior

/**
 * Main class for server startup.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object Main extends Logging {
   
   val PROP_KEY_PORT = "infinispan.server.port"
   val PROP_KEY_HOST = "infinispan.server.host"
   val PROP_KEY_MASTER_THREADS = "infinispan.server.master_threads"
   val PROP_KEY_WORKER_THREADS = "infinispan.server.worker_threads"
   val PROP_KEY_CACHE_CONFIG = "infinispan.server.cache_config"
   val PROP_KEY_PROTOCOL = "infinispan.server.protocol"
   val PROP_KEY_IDLE_TIMEOUT = "infinispan.server.idle_timeout"
   val PROP_KEY_TCP_NO_DELAY = "infinispan.server.tcp_no_delay"
   val PROP_KEY_SEND_BUF_SIZE = "infinispan.server.send_buf_size"
   val PROP_KEY_RECV_BUF_SIZE = "infinispan.server.recv_buf_size"
   val PROP_KEY_PROXY_HOST = "infinispan.server.proxy_host"
   val PROP_KEY_PROXY_PORT = "infinispan.server.proxy_port"
   val HOST_DEFAULT = "127.0.0.1"
   val MASTER_THREADS_DEFAULT = 0
   val WORKER_THREADS_DEFAULT = 0
   val IDLE_TIMEOUT_DEFAULT = -1
   val TCP_NO_DELAY_DEFAULT = true
   val SEND_BUF_SIZE_DEFAULT = 0
   val RECV_BUF_SIZE_DEFAULT = 0

   /**
    * Server properties.  This object holds all of the required
    * information to get the server up and running. Use System
    * properties for defaults.
    */
   private val props: Properties = new TypedProperties(System.getProperties)
   
   private var server: ProtocolServer = _

   private var cacheManager: EmbeddedCacheManager = _

   def getServer = server

   def getCacheManager = cacheManager

   def main(args: Array[String]) {
      info("Start main with args: {0}", args.mkString(", "))
      val worker = new Callable[Void] {
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
      val f = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
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

      var protocol = props.getProperty(PROP_KEY_PROTOCOL)
      if (protocol == null) {
         System.err.println("ERROR: Please indicate protocol to run with -r parameter")
         showAndExit
      }
      
      // TODO: move class name and protocol number to a resource file under the corresponding project
      val clazz = protocol match {
         case "memcached" => "org.infinispan.server.memcached.MemcachedServer"
         case "hotrod" => "org.infinispan.server.hotrod.HotRodServer"
         case "websocket" => "org.infinispan.server.websocket.WebSocketServer"
      }
      server = Util.getInstance(clazz).asInstanceOf[ProtocolServer]

      val configFile = props.getProperty(PROP_KEY_CACHE_CONFIG)
      cacheManager = if (configFile == null) createCacheManagerNoConfig else new DefaultCacheManager(configFile)
      // Servers need a shutdown hook to close down network layer, so there's no need for an extra shutdown hook.
      // Removing Infinispan's hook also makes shutdown procedures for server and cache manager sequential, avoiding
      // issues with having the JGroups channel disconnected before it's removed itself from the topology view.
      cacheManager.getGlobalConfiguration.setShutdownHookBehavior(ShutdownHookBehavior.DONT_REGISTER)
      addShutdownHook(new ShutdownHook(server, cacheManager))
      server.start(props, cacheManager)
   }

   private def createCacheManagerNoConfig: EmbeddedCacheManager = {
      val globalCfg = new GlobalConfiguration
      globalCfg.setExposeGlobalJmxStatistics(true)
      val defaultCfg = new Configuration
      defaultCfg.setExposeJmxStatistics(true)
      new DefaultCacheManager(globalCfg, defaultCfg)
   }

   private def processCommandLine(args: Array[String]) {
      val sopts = "-:hD:Vp:l:m:t:c:r:i:n:s:e:o:x:"
      val lopts = Array(
         new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h'),
         new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V'),
         new LongOpt("port", LongOpt.REQUIRED_ARGUMENT, null, 'p'),
         new LongOpt("host", LongOpt.REQUIRED_ARGUMENT, null, 'l'),
         new LongOpt("master_threads", LongOpt.REQUIRED_ARGUMENT, null, 'm'),
         new LongOpt("worker_threads", LongOpt.REQUIRED_ARGUMENT, null, 't'),
         new LongOpt("cache_config", LongOpt.REQUIRED_ARGUMENT, null, 'c'),
         new LongOpt("protocol", LongOpt.REQUIRED_ARGUMENT, null, 'r'),
         new LongOpt("idle_timeout", LongOpt.REQUIRED_ARGUMENT, null, 'i'),
         new LongOpt("tcp_no_delay", LongOpt.REQUIRED_ARGUMENT, null, 'n'),
         new LongOpt("send_buf_size", LongOpt.REQUIRED_ARGUMENT, null, 's'),
         new LongOpt("recv_buf_size", LongOpt.REQUIRED_ARGUMENT, null, 'e'),
         new LongOpt("proxy_host", LongOpt.REQUIRED_ARGUMENT, null, 'o'),
         new LongOpt("proxy_port", LongOpt.REQUIRED_ARGUMENT, null, 'x')
         )
      val getopt = new Getopt("startServer", args, sopts, lopts)
      var code: Int = 0
      while ((({code = getopt.getopt; code})) != -1) {
         code match {
            case ':' | '?' => System.exit(1)
            case 1 => System.err.println("startServer: unused non-option argument: " + getopt.getOptarg)
            case 'h' => showAndExit
            case 'V' => {
               Version.printFullVersionInformation
               System.exit(0)
            }
            case 'p' => props.setProperty(PROP_KEY_PORT, getopt.getOptarg)
            case 'l' => props.setProperty(PROP_KEY_HOST, getopt.getOptarg)
            case 'm' => props.setProperty(PROP_KEY_MASTER_THREADS, getopt.getOptarg)
            case 't' => props.setProperty(PROP_KEY_WORKER_THREADS, getopt.getOptarg)
            case 'c' => props.setProperty(PROP_KEY_CACHE_CONFIG, getopt.getOptarg)
            case 'r' => props.setProperty(PROP_KEY_PROTOCOL, getopt.getOptarg)
            case 'i' => props.setProperty(PROP_KEY_IDLE_TIMEOUT, getopt.getOptarg)
            case 'n' => props.setProperty(PROP_KEY_TCP_NO_DELAY, getopt.getOptarg)
            case 's' => props.setProperty(PROP_KEY_SEND_BUF_SIZE, getopt.getOptarg)
            case 'e' => props.setProperty(PROP_KEY_RECV_BUF_SIZE, getopt.getOptarg)
            case 'o' => props.setProperty(PROP_KEY_PROXY_HOST, getopt.getOptarg)
            case 'x' => props.setProperty(PROP_KEY_PROXY_PORT, getopt.getOptarg)
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
      println("usage: startServer [options]")
      println
      println("options:")
      println("    -h, --help                         Show this help message")
      println
      println("    -V, --version                      Show version information")
      println
      println("    --                                 Stop processing options")
      println
      println("    -p, --port=<num>                   TCP port number to listen on (default: 11211 for Memcached, 11222 for Hot Rod and 8181 for WebSocket server)")
      println
      println("    -l, --host=<host or ip>            Interface to listen on (default: 127.0.0.1, localhost)")
      println
      println("    -m, --master_threads=<num>         Number of threads accepting incoming connections (default: unlimited while resources are available)")
      println
      println("    -t, --work_threads=<num>           Number of threads processing incoming requests and sending responses (default: unlimited while resources are available)")
      println
      println("    -c, --cache_config=<filename>      Cache configuration file (default: creates cache with default values)")
      println
      println("    -r, --protocol=                    Protocol to understand by the server. This is a mandatory option and you should choose one of these options")
      println("          [memcached|hotrod|websocket]")
      println
      println("    -i, --idle_timeout=<num>           Idle read timeout, in seconds, used to detect stale connections (default: -1).")
      println("                                       If no new messages have been read within this time, the server disconnects the channel.")
      println("                                       Passing -1 disables idle timeout.")
      println
      println("    -n, --tcp_no_delay=[true|false]    TCP no delay flag switch (default: true).")
      println
      println("    -s, --send_buf_size=<num>          Send buffer size (default: as defined by the OS).")
      println
      println("    -e, --recv_buf_size=<num>          Receive buffer size (default: as defined by the OS).")
      println
      println("    -o, --proxy_host=<host or ip>      Host address to expose in topology information sent to clients. If not present, it defaults to configured host. Servers that do not transmit topology information ignore this setting.")
      println
      println("    -x, --proxy_port=<num>             Port to expose in topology information sent to clients. If not present, it defaults to configured port. Servers that do not transmit topology information ignore this setting.")
      println
      println("    -D<name>[=<value>]                 Set a system property")
      println
      System.exit(0)
   }
}

private class ShutdownHook(server: ProtocolServer, cacheManager: CacheContainer) extends Thread with Logging {
   override def run {
      if (server != null) {
         info("Posting Shutdown Request to the server...")
         val tf = new ThreadFactory {
            override def newThread(r: Runnable): Thread = new Thread(r, "StopThread")
         }

         val f = Executors.newSingleThreadExecutor(tf).submit(new Callable[Void] {
            override def call = {
               // Stop server first so that no new requests are allowed
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
