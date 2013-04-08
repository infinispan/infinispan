/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
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
package org.infinispan.server.core

import java.io.IOException
import java.security.{PrivilegedAction, AccessController}
import java.util.concurrent.{ThreadFactory, ExecutionException, Callable, Executors}
import gnu.getopt.{Getopt, LongOpt}
import logging.Log
import org.infinispan.Version
import java.util.Properties
import org.infinispan.util.{TypedProperties, Util}
import org.infinispan.config.{Configuration, GlobalConfiguration}
import org.infinispan.manager.{EmbeddedCacheManager, CacheContainer, DefaultCacheManager}
import org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior
import org.infinispan.util.logging.LogFactory
import org.infinispan.server.core.configuration.ProtocolServerConfiguration
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder

/**
 * Main class for server startup.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object Main extends Log {

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
   val PROP_KEY_TOPOLOGY_LOCK_TIMEOUT = "infinispan.server.topology.lock_timeout"
   val PROP_KEY_TOPOLOGY_REPL_TIMEOUT = "infinispan.server.topology.repl_timeout"
   val PROP_KEY_TOPOLOGY_STATE_TRANSFER = "infinispan.server.topology.state_transfer"
   val PROP_KEY_TOPOLOGY_UPDATE_TIMEOUT = "infinispan.server.topology.update_timeout"
   val PROP_KEY_CACHE_MANAGER_CLASS = "infinispan.server.cache_manager_class"
   val HOST_DEFAULT = "127.0.0.1"
   val WORKER_THREADS_DEFAULT = 2 * Runtime.getRuntime.availableProcessors()
   val IDLE_TIMEOUT_DEFAULT = -1
   val TCP_NO_DELAY_DEFAULT = true
   val SEND_BUF_SIZE_DEFAULT = 0
   val RECV_BUF_SIZE_DEFAULT = 0
   val TOPO_LOCK_TIMEOUT_DEFAULT = 10000L
   val TOPO_REPL_TIMEOUT_DEFAULT = 10000L
   val TOPO_UPDATE_TIMEOUT_DEFAULT = 30000L
   val TOPO_STATE_TRANSFER_DEFAULT = true

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
      logStartWithArgs(args.mkString(", "))
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
      val serverClazz = protocol match {
         case "memcached" => "org.infinispan.server.memcached.MemcachedServer"
         case "hotrod" => "org.infinispan.server.hotrod.HotRodServer"
         case "websocket" => "org.infinispan.server.websocket.WebSocketServer"
      }
      server = Util.getInstance(serverClazz, Thread.currentThread().getContextClassLoader()).asInstanceOf[ProtocolServer]

      val configFile = props.getProperty(PROP_KEY_CACHE_CONFIG)
      cacheManager = instantiateCacheManager(configFile)
      // Servers need a shutdown hook to close down network layer, so there's no need for an extra shutdown hook.
      // Removing Infinispan's hook also makes shutdown procedures for server and cache manager sequential, avoiding
      // issues with having the JGroups channel disconnected before it's removed itself from the topology view.
      cacheManager.getGlobalConfiguration.setShutdownHookBehavior(ShutdownHookBehavior.DONT_REGISTER)
      addShutdownHook(new ShutdownHook(server, cacheManager))
      server.startWithProperties(props, cacheManager)
   }

   private def instantiateCacheManager(configFile: String): EmbeddedCacheManager = {
	   var clazzName = props.getProperty(PROP_KEY_CACHE_MANAGER_CLASS)
	   val clazz =
         if (null == clazzName)
	  	      classOf[DefaultCacheManager]
	      else
	  	      Class.forName(clazzName)

	   if (null == configFile)
	  	   createCacheManagerNoConfig(clazz.asInstanceOf[Class[EmbeddedCacheManager]])
	   else
	  	   createCacheManager(clazz.asInstanceOf[Class[EmbeddedCacheManager]], configFile)
   }

   private def createCacheManagerNoConfig(clazz: Class[EmbeddedCacheManager]): EmbeddedCacheManager = {
      val globalCfg = new GlobalConfiguration
      globalCfg.setExposeGlobalJmxStatistics(true)
      val defaultCfg = new Configuration
      defaultCfg.setExposeJmxStatistics(true)
      try {
    	  val constructor = clazz.getConstructor(classOf[GlobalConfiguration], classOf[Configuration])
    	  return constructor.newInstance(globalCfg, defaultCfg)
      } catch {
	  	   case e: NoSuchMethodException => {
            throw new Exception(
               "%s does not have a constructor that takes GlobalConfiguration and Configuration instances".format(clazz), e)
	  	   }
	   }
   }

   private def createCacheManager(clazz: Class[EmbeddedCacheManager], configFile: String): EmbeddedCacheManager = {
	   try {
		   val constructor = clazz.getConstructor(classOf[String])
		   return constructor.newInstance(configFile)
	   } catch {
	  	   case e: NoSuchMethodException => {
            throw new Exception(
              "%s does not have a constructor that takes a config file path".format(clazz), e)
	  	   }
	   }
   }

   private def processCommandLine(args: Array[String]) {
      val sopts = "-:hD:Vp:l:m:t:c:r:i:n:s:e:o:x:k:u:a:f:d:"
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
         new LongOpt("topo_lock_timeout", LongOpt.REQUIRED_ARGUMENT, null, 'k'),
         new LongOpt("topo_repl_timeout", LongOpt.REQUIRED_ARGUMENT, null, 'u'),
         new LongOpt("topo_state_transfer", LongOpt.REQUIRED_ARGUMENT, null, 'a'),
         new LongOpt("topo_update_time", LongOpt.REQUIRED_ARGUMENT, null, 'd'),
         new LongOpt("cache_manager_class", LongOpt.REQUIRED_ARGUMENT, null, 'f')
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
            case 'k' => props.setProperty(PROP_KEY_TOPOLOGY_LOCK_TIMEOUT, getopt.getOptarg)
            case 'u' => props.setProperty(PROP_KEY_TOPOLOGY_REPL_TIMEOUT, getopt.getOptarg)
            case 'a' => props.setProperty(PROP_KEY_TOPOLOGY_STATE_TRANSFER, getopt.getOptarg)
            case 'd' => props.setProperty(PROP_KEY_TOPOLOGY_UPDATE_TIMEOUT, getopt.getOptarg)
            case 'f' => props.setProperty(PROP_KEY_CACHE_MANAGER_CLASS, getopt.getOptarg)
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
      println("    -t, --worker_threads=<num>         Number of threads processing incoming requests and sending responses (default: 20 * number of processors)")
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
      println("    -o, --proxy_host=<host or ip>      Host address to expose in topology information sent to clients.")
      println("                                       If not present, it defaults to configured host.")
      println("                                       Servers that do not transmit topology information ignore this setting.")
      println
      println("    -x, --proxy_port=<num>             Port to expose in topology information sent to clients. If not present, it defaults to configured port.")
      println("                                       Servers that do not transmit topology information ignore this setting.")
      println
      println("    -k, --topo_lock_timeout=<num>      Controls lock timeout (in milliseconds) for those servers that maintain the topology information in an internal cache.")
      println
      println("    -u, --topo_repl_timeout=<num>      Sets the maximum replication time (in milliseconds) for transfer of topology information between servers.")
      println("                                       If state transfer is enabled, this setting also controls the topology cache state transfer timeout.")
      println("                                       If state transfer is disabled, it controls the amount of time to wait for this topology data")
      println("                                       to be lazily loaded from a different node when not present locally.")
      println("                                       This value should be set higher than 'topo_lock_timeout' to allow remote locks to be acquired within the time allocated to replicate the topology.")
      println
      println("    -a, --topo_state_trasfer=          Enabling topology information state transfer means that when a server starts it retrieves this information from a different node.")
      println("          [true|false]                 Otherwise, if set to false, the topology information is lazily loaded if not available locally.")
      println
      println("    -d, --topo_update_timeout=<num>    Sets the maximum time (in milliseconds) to wait for topology information to be updated.")
      println("                                       This value should be set higher than 'topo_repl_timeout' to allow retries to happen if a replication timeout is encountered.")
      println
      println("    -f, --cache_manager_class=<clazz>  Cache manager class name to be used instead of the default one (it has to extend org.infinispan.manager.EmbeddedCacheManager).")
      println
      println("    -D<name>[=<value>]                 Set a system property")
      println
      System.exit(0)
   }
}

private class ShutdownHook(server: ProtocolServer, cacheManager: CacheContainer) extends Thread with Log {

   // Constructor code inline
   setName("ShutdownHookThread")

   private lazy val log: Log = LogFactory.getLog(getClass, classOf[Log])

   override def run {
      if (server != null) {
         logPostingShutdownRequest
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
