/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.rest

import com.thoughtworks.xstream.XStream
import java.io._
import java.util.Date
import java.util.concurrent.TimeUnit.{MILLISECONDS => MILLIS}
import java.util.concurrent.TimeUnit.{SECONDS => SECS}
import javax.ws.rs._
import core._
import core.Response.{ResponseBuilder, Status}
import org.infinispan.api.BasicCacheContainer
import org.infinispan.manager._
import org.codehaus.jackson.map.ObjectMapper
import org.infinispan.{CacheException, AdvancedCache}
import org.infinispan.commons.hash.MurmurHash3
import javax.ws.rs._
import javax.servlet.http.HttpServletResponse
import javax.servlet.ServletContext
import scala.collection.JavaConverters._
import scala.xml.Utility
import org.infinispan.tasks.GlobalKeySetTask
import org.infinispan.util.CollectionFactory
import org.infinispan.container.entries.CacheEntry
import org.infinispan.container.entries.InternalCacheEntry
import org.infinispan.rest.configuration.ExtendedHeaders
import org.infinispan.remoting.transport.jgroups.JGroupsTransport
import org.infinispan.distribution.DistributionManager
import org.infinispan.remoting.transport.Address
import org.infinispan.configuration.cache.Configuration
import org.infinispan.metadata.Metadata

/**
 * Integration server linking REST requests with Infinispan calls.
 *
 * @author Michael Neale
 * @author Galder Zamarreño
 * @since 4.0
 */
@Path("/rest")
class Server(@Context request: Request, @Context servletContext: ServletContext, @HeaderParam("performAsync") useAsync: Boolean) {

   /**For dealing with binary entries in the cache */
   lazy val variantList = Variant.VariantListBuilder.newInstance.mediaTypes(MediaType.APPLICATION_XML_TYPE, MediaType.APPLICATION_JSON_TYPE).build
   lazy val collectionVariantList = Variant.VariantListBuilder.newInstance.mediaTypes(
            MediaType.TEXT_HTML_TYPE,
            MediaType.APPLICATION_XML_TYPE,
            MediaType.APPLICATION_JSON_TYPE,
            MediaType.TEXT_PLAIN_TYPE
         ).build
   lazy val jsonMapper = new ObjectMapper
   lazy val xstream = new XStream
   val manager = ServerBootstrap.getManagerInstance(servletContext)
   val configuration = ServerBootstrap.getConfiguration(servletContext)

   @GET
   @Path("/{cacheName}")
   def getKeys(@PathParam("cacheName") cacheName: String, @QueryParam("global") globalKeySet: String): Response = {
      protectCacheNotFound(request, useAsync) { (request, useAsync) => {
         val cache = manager.getCache(cacheName)
         val keys = (if (globalKeySet !=null) GlobalKeySetTask.getGlobalKeySet(cache) else cache.keySet()).asScala
         val variant = request.selectVariant(collectionVariantList)
         val selectedMediaType = if (variant != null) variant.getMediaType.toString else null
         selectedMediaType match {
            case MediaType.TEXT_HTML => Response.ok.`type`(MediaType.TEXT_HTML).entity(printIt( pw => {
               pw.print("<html><body>")
               keys.foreach(key => {
                  val hkey = Escaper.escapeHtml(key)
                  pw.printf("<a href=\"%s\">%s</a><br/>", hkey, hkey)
               })
               pw.print("</body></html>")
            })).build
            case MediaType.APPLICATION_XML => Response.ok.`type`(MediaType.APPLICATION_XML).entity(printIt( pw => {
               pw.print("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n<keys>")
               keys.foreach(key => pw.printf("<key>%s</key>", Escaper.escapeXml(key)))
               pw.print("</keys>")
            })).build
            case MediaType.APPLICATION_JSON => Response.ok.`type`(MediaType.APPLICATION_JSON).entity(printIt( pw => {
               pw.print("keys=[")
               val it = keys.iterator
               while (it.hasNext) {
                  pw.printf("\"%s\"", Escaper.escapeJson(it.next()))
                  if (it.hasNext) pw.print(",")

               }
               pw.print("]")
            })).build
            case MediaType.TEXT_PLAIN => Response.ok.`type`(MediaType.TEXT_PLAIN).entity(printIt( pw => keys.foreach(pw.println(_)) )).build
            case null => Response.notAcceptable(collectionVariantList).build
         }
      }
      }
   }

   @GET
   @Path("/{cacheName}/{cacheKey}")
   def getEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String, @QueryParam("extended") extended: String): Response = {
      protectCacheNotFound(request, useAsync) { (request, useAsync) =>
         manager.getInternalEntry(cacheName, key) match {
            case ice: InternalCacheEntry => {
               val lastMod = lastModified(ice)
               val expires = if (ice.canExpire) new Date(ice.getExpiryTime) else null
               ice.getMetadata match {
                  case meta: MimeMetadata =>
                     getMimeEntry(ice, meta, lastMod, expires, cacheName, extended)
                  case _ =>
                     getAnyEntry(ice, lastMod, expires, cacheName, extended)
               }
            }
            case _ => Response.status(Status.NOT_FOUND).build
         }
      }
   }

   private def getMimeEntry(ice: InternalCacheEntry, meta: MimeMetadata,
           lastMod: Date, expires: Date, cacheName: String, extended: String): Response = {
      val key = ice.getKey.asInstanceOf[String]
      request.evaluatePreconditions(lastMod, calcETAG(ice, meta)) match {
         case bldr: ResponseBuilder => bldr.build
         case null => Response.ok(ice.getValue, meta.contentType)
                 .lastModified(lastMod)
                 .expires(expires)
                 .tag(calcETAG(ice, meta))
                 .extended(cacheName, key, wantExtendedHeaders(extended)).build
      }
   }

   private def getAnyEntry(ice: InternalCacheEntry, lastMod: Date, expires: Date,
           cacheName: String, extended: String): Response = {
      val key = ice.getKey.asInstanceOf[String]
      ice.getValue match {
         case s: String => Response.ok(s, "text/plain").build
         case obj: Any => {
            val variant = request.selectVariant(variantList)
            val selectedMediaType =
               if (variant != null) variant.getMediaType.toString
               else "application/x-java-serialized-object"

            selectedMediaType match {
               case MediaType.APPLICATION_JSON => Response.ok
                       .`type`(selectedMediaType)
                       .lastModified(lastMod)
                       .expires(expires)
                       .extended(cacheName, key, wantExtendedHeaders(extended))
                       .entity(streamIt(jsonMapper.writeValue(_, obj)))
                       .build
               case MediaType.APPLICATION_XML => Response.ok
                       .`type`(selectedMediaType)
                       .lastModified(lastMod)
                       .expires(expires)
                       .extended(cacheName, key, wantExtendedHeaders(extended))
                       .entity(streamIt(xstream.toXML(obj, _)))
                       .build
               case _ =>
                  obj match {
                     case ba: Array[Byte] => Response.ok
                             .`type`("application/x-java-serialized-object")
                             .lastModified(lastMod)
                             .expires(expires)
                             .extended(cacheName, key, wantExtendedHeaders(extended))
                             .entity(streamIt(_.write(ba)))
                             .build
                     case ser: Serializable => Response.ok
                             .`type`("application/x-java-serialized-object")
                             .lastModified(lastMod)
                             .expires(expires)
                             .extended(cacheName, key, wantExtendedHeaders(extended))
                             .entity(streamIt(new ObjectOutputStream(_).writeObject(ser)))
                             .build
                     case _ => Response.notAcceptable(variantList).build
                  }
            }
         }
      }
   }

   /**
    * The implicit below allows us to add custom methods to RestEasy's ResponseBuilder
    * so as not to interrupt the chain of invocations
    */
   implicit private class ResponseBuilderExtender(val bld: Response.ResponseBuilder) {
      def extended(cacheName: String, key: String, b: Boolean) = {
         if (b) {
            bld
               .header("Cluster-Primary-Owner", manager.getPrimaryOwner(cacheName, key))
               .header("Cluster-Node-Name", manager.getNodeName)
               .header("Cluster-Server-Address", manager.getServerAddress)
         } else
            bld
      }
   }

   private def wantExtendedHeaders(extended: String): Boolean = configuration.extendedHeaders() match {
      case ExtendedHeaders.NEVER => false
      case ExtendedHeaders.ON_DEMAND => extended != null
   }

   /**create a JAX-RS streaming output */
   def streamIt(action: (OutputStream) => Unit) = new StreamingOutput {
      def write(o: OutputStream) { action(o) }
   }

   def printIt(action: (PrintWriter) => Unit) = new StreamingOutput {
      def write(o: OutputStream) {
         val pw = new PrintWriter(o)
         try {
            action(pw)
         } finally {
            pw.flush()
         }
      }
   }

   @HEAD
   @Path("/{cacheName}/{cacheKey}")
   def headEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String, @QueryParam("extended") extended: String): Response = {
      protectCacheNotFound(request, useAsync) { (request, useAsync) =>
         manager.getInternalEntry(cacheName, key) match {
            case ice: InternalCacheEntry => {
               val lastMod = lastModified(ice)
               val expires = if (ice.canExpire) new Date(ice.getExpiryTime) else null
               ice.getMetadata match {
                  case meta: MimeMetadata =>
                     request.evaluatePreconditions(lastMod, calcETAG(ice, meta)) match {
                        case bldr: ResponseBuilder => bldr.build
                        case null => Response.ok
                                .`type`(meta.contentType)
                                .lastModified(lastMod)
                                .expires(expires)
                                .tag(calcETAG(ice, meta))
                                .extended(cacheName, key, wantExtendedHeaders(extended))
                                .build
                     }
                  case _ => Response.ok
                          .lastModified(lastMod)
                          .expires(expires)
                          .extended(cacheName, key, wantExtendedHeaders(extended))
                          .build
               }
            }
            case _ => Response.status(Status.NOT_FOUND).build
         }
      }
   }

   @PUT
   @POST
   @Path("/{cacheName}/{cacheKey}")
   def putEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String,
                @HeaderParam("Content-Type") mediaType: String, data: Array[Byte],
                @DefaultValue("-1") @HeaderParam("timeToLiveSeconds") ttl: Long,
                @DefaultValue("-1") @HeaderParam("maxIdleTimeSeconds") idleTime: Long): Response = {
      protectCacheNotFound(request, useAsync) { (request, useAsync) =>
         val cache = manager.getCache(cacheName)
         if (request.getMethod == "POST" && cache.containsKey(key)) {
            Response.status(Status.CONFLICT).build()
         } else {
            manager.getInternalEntry(cacheName, key) match {
               case ice: InternalCacheEntry => {
                  val lastMod = lastModified(ice)
                  ice.getMetadata match {
                     case mime: MimeMetadata =>
                        // The item already exists in the cache, evaluate preconditions based on its attributes and the headers
                        val etag = calcETAG(ice, mime)
                        request.evaluatePreconditions(lastMod, etag) match {
                           // One of the preconditions failed, build a response
                           case bldr: ResponseBuilder => bldr.build
                           // Preconditions passed
                           case null => putInCache(cache, key, data, mediaType,
                              ttl, idleTime, Some(ice.getValue.asInstanceOf[Array[Byte]]))
                        }
                     case _ =>
                        putInCache(cache, key, data, mediaType, ttl, idleTime, None)
                  }
               }
               case _ =>
                  putInCache(cache, key, data, mediaType, ttl, idleTime, None)
            }
         }
      }
   }

   private def putInCache(cache: AdvancedCache[String, Array[Byte]], key: String,
           data: Array[Byte], dataType: String, ttl: Long, idleTime: Long,
           prevCond: Option[Array[Byte]]): Response = {
      if (useAsync)
         asyncPutInCache(cache, key, data, dataType, ttl, idleTime)
      else
         putOrReplace(cache, key, data, dataType, ttl, idleTime, prevCond)
   }

   def asyncPutInCache(cache: AdvancedCache[String, Array[Byte]],
           key: String, data: Array[Byte], dataType: String,
           ttl: Long, idleTime: Long): Response = {
      val metadata = createMetadata(cache.getCacheConfiguration, dataType, ttl, idleTime)
      cache.putAsync(key, data, metadata)
      Response.ok.build
   }

   def createMetadata(cfg: Configuration, dataType: String, ttl: Long, idleTime: Long): Metadata = {
      (ttl, idleTime) match {
         case (0, 0) => MimeMetadata(dataType,
            cfg.expiration().lifespan(), MILLIS,
            cfg.expiration().maxIdle(), MILLIS)
         case (lifespan, 0) =>
            MimeMetadata(dataType, lifespan, SECS, cfg.expiration().maxIdle(), MILLIS)
         case (lifespan, maxIdle) =>
            MimeMetadata(dataType, lifespan, SECS, maxIdle, SECS)
      }
   }

   private def putOrReplace(cache: AdvancedCache[String, Array[Byte]],
           key: String, data: Array[Byte], dataType: String,
           ttl: Long, idleTime: Long,
           prevCond: Option[Array[Byte]]): Response = {
      val metadata = createMetadata(cache.getCacheConfiguration, dataType, ttl, idleTime)
      prevCond match {
         case None =>
            cache.put(key, data, metadata)
            Response.ok.build
         case Some(prev) =>
            val replaced = cache.replace(key, prev, data, metadata)
            // If not replaced, simply send back that the precondition failed
            if (replaced) Response.ok.build
            else Response.status(
               HttpServletResponse.SC_PRECONDITION_FAILED).build()
      }
   }

   @DELETE
   @Path("/{cacheName}/{cacheKey}")
   def removeEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String): Response = {
      manager.getInternalEntry(cacheName, key) match {
         case ice: InternalCacheEntry => {
            val lastMod = lastModified(ice)
            ice.getMetadata match {
               case meta: MimeMetadata =>
                  // The item exists in the cache, evaluate preconditions based on its attributes and the headers
                  val etag = calcETAG(ice, meta)
                  request.evaluatePreconditions(lastMod, etag) match {
                     // One of the preconditions failed, build a response
                     case bldr: ResponseBuilder => bldr.build
                     // Preconditions passed
                     case _ => {
                        if (useAsync) {
                           manager.getCache(cacheName).removeAsync(key)
                        } else {
                           manager.getCache(cacheName).remove(key)
                        }
                        Response.ok.build
                     }
                  }
               case _ =>
                  if (useAsync) {
                     manager.getCache(cacheName).removeAsync(key)
                  } else {
                     manager.getCache(cacheName).remove(key)
                  }
                  Response.ok.build

            }
         }
         case null => Response.ok.build
      }
   }

   @DELETE
   @Path("/{cacheName}")
   def killCache(@PathParam("cacheName") cacheName: String,
                 @DefaultValue("") @HeaderParam("If-Match") ifMatch: String,
                 @DefaultValue("") @HeaderParam("If-None-Match") ifNoneMatch: String,
                 @DefaultValue("") @HeaderParam("If-Modified-Since") ifModifiedSince: String,
                 @DefaultValue("") @HeaderParam("If-Unmodified-Since") ifUnmodifiedSince: String): Response = {
      if (ifMatch.isEmpty && ifNoneMatch.isEmpty && ifModifiedSince.isEmpty && ifUnmodifiedSince.isEmpty) {
         manager.getCache(cacheName).clear()
         Response.ok.build
      } else {
         preconditionNotImplementedResponse()
      }
   }

   private def preconditionNotImplementedResponse() = {
      Response.status(501).entity(
         "Preconditions were not implemented yet for PUT, POST, and DELETE methods.").build()
   }

   val hashFunc = new MurmurHash3()

   private def calcETAG(entry: InternalCacheEntry, meta: MimeMetadata): EntityTag =
      new EntityTag(meta.contentType + hashFunc.hash(entry.getValue))

   private def lastModified(ice: InternalCacheEntry): Date = { new Date(ice.getCreated / 1000 * 1000) }

   private def protectCacheNotFound(request: Request, useAsync: Boolean) (op: (Request, Boolean) => Response): Response = {
      try {
         op(request, useAsync)
      } catch {
         case e: CacheNotFoundException =>
            Response.status(Status.NOT_FOUND).build
      }
   }

}

/**
 * Just wrap a single instance of the Infinispan cache manager.
 */
class ManagerInstance(instance: EmbeddedCacheManager) {
   private[rest] val knownCaches : java.util.Map[String, AdvancedCache[String, Array[Byte]]] =
      CollectionFactory.makeConcurrentMap(4, 0.9f, 16)

   def getCache(name: String): AdvancedCache[String, Array[Byte]] = {
      val isKnownCache = knownCaches.containsKey(name)
      if (name != BasicCacheContainer.DEFAULT_CACHE_NAME && !isKnownCache && !instance.getCacheNames.contains(name))
         throw new CacheNotFoundException("Cache with name '" + name + "' not found amongst the configured caches")

      if (isKnownCache) {
         knownCaches.get(name)
      } else {
         val rv =
            if (name == BasicCacheContainer.DEFAULT_CACHE_NAME)
               instance.getCache[String, Array[Byte]]().getAdvancedCache
            else
               instance.getCache[String, Array[Byte]](name).getAdvancedCache

         knownCaches.put(name, rv)
         rv
      }
   }

   def getEntry(cacheName: String, key: String): Array[Byte] = getCache(cacheName).get(key)

   def getInternalEntry(cacheName: String, key: String): CacheEntry = getCache(cacheName).getCacheEntry(key)

   def getNodeName: Address = instance.getAddress

   def getServerAddress: String =
      instance.getTransport match {
         case trns: JGroupsTransport => trns.getPhysicalAddresses.toString
         case null => null
      }

   def getPrimaryOwner(cacheName: String, key: String): String =
      getCache(cacheName).getDistributionManager match {
         case dm: DistributionManager => dm.getPrimaryLocation(key).toString
         case null => null
      }

   def getInstance = instance

}

class CacheNotFoundException(msg: String) extends CacheException(msg)

object Escaper {
   def escapeHtml(html: String): String = {
      Utility.escape(html)
   }

   def escapeXml(xml: String): String = {
      Utility.escape(xml)
   }

   def escapeJson(json: String): String = {
      json.replaceAll("\"", "\\\\\"")
   }
}
