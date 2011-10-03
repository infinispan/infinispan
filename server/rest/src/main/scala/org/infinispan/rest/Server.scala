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
import java.util.concurrent.TimeUnit
import javax.ws.rs._
import core._
import core.Response.{ResponseBuilder, Status}
import org.infinispan.remoting.MIMECacheEntry
import org.infinispan.manager._
import org.codehaus.jackson.map.ObjectMapper
import org.infinispan.{CacheException, Cache}
import org.infinispan.util.hash.MurmurHash3

/**
 * Integration server linking REST requests with Infinispan calls.
 *
 * @author Michael Neale
 * @author Galder Zamarreño
 * @since 4.0
 */
@Path("/rest")
class Server(@Context request: Request, @HeaderParam("performAsync") useAsync: Boolean) {

   /**For dealing with binary entries in the cache */
   lazy val variantList = Variant.VariantListBuilder.newInstance.mediaTypes(MediaType.APPLICATION_XML_TYPE, MediaType.APPLICATION_JSON_TYPE).build
   lazy val jsonMapper = new ObjectMapper
   lazy val xstream = new XStream

   @GET
   @Path("/{cacheName}/{cacheKey}")
   def getEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String): Response = {
      protectCacheNotFound(request, useAsync) { (request, useAsync) =>
         ManagerInstance.getEntry(cacheName, key) match {
            case b: MIMECacheEntry => {
               val lastMod = new Date(b.lastModified)
               request.evaluatePreconditions(lastMod, calcETAG(b)) match {
                  case bldr: ResponseBuilder => bldr.build
                  case null => Response.ok(b.data, b.contentType).lastModified(lastMod).tag(calcETAG(b)).build
               }
            }
            case s: String => Response.ok(s, "text/plain").build
            case obj: Any => {
               val variant = request.selectVariant(variantList)
               val selectedMediaType = if (variant != null) variant.getMediaType.toString else "application/x-java-serialized-object"
               selectedMediaType match {
                  case MediaType.APPLICATION_JSON => Response.ok.`type`(selectedMediaType).entity(streamIt(jsonMapper.writeValue(_, obj))).build
                  case MediaType.APPLICATION_XML => Response.ok.`type`(selectedMediaType).entity(streamIt(xstream.toXML(obj, _))).build
                  case _ =>
                     obj match {
                        case ba: Array[Byte] =>
                           Response.ok.`type`("application/x-java-serialized-object").entity(streamIt(_.write(ba))).build
                        case ser: Serializable =>
                           Response.ok.`type`("application/x-java-serialized-object").entity(streamIt(new ObjectOutputStream(_).writeObject(ser))).build
                        case _ => Response.notAcceptable(variantList).build
                     }

               }
            }
            case null => Response status (Status.NOT_FOUND) build
            case _ => throw new Exception
         }
      }
   }

   /**create a JAX-RS streaming output */
   def streamIt(action: (OutputStream) => Unit) = new StreamingOutput {def write(o: OutputStream) = {action(o)}}

   @HEAD
   @Path("/{cacheName}/{cacheKey}")
   def headEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String): Response = {
      protectCacheNotFound(request, useAsync) { (request, useAsync) =>
         ManagerInstance.getEntry(cacheName, key) match {
            case b: MIMECacheEntry => {
               val lastMod = new Date(b.lastModified)
               request.evaluatePreconditions(lastMod, calcETAG(b)) match {
                  case bldr: ResponseBuilder => bldr.build
                  case null => Response.ok.`type`(b.contentType).lastModified(lastMod).tag(calcETAG(b)).build
               }
            }
            case x: Any => Response.ok.build
            case null => Response status (Status.NOT_FOUND) build
         }
      }
   }

   @PUT
   @POST
   @Path("/{cacheName}/{cacheKey}")
   def putEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String,
                @HeaderParam("Content-Type") mediaType: String, data: Array[Byte],
                @DefaultValue("0") @HeaderParam("timeToLiveSeconds") ttl: Long,
                @DefaultValue("0") @HeaderParam("maxIdleTimeSeconds") idleTime: Long): Response = {
      protectCacheNotFound(request, useAsync) { (request, useAsync) =>
         val cache = ManagerInstance.getCache(cacheName)
         if (request.getMethod == "POST" && cache.containsKey(key)) {
            Response.status(Status.CONFLICT).build()
         } else {
            ManagerInstance.getEntry(cacheName, key) match {
               case b: MIMECacheEntry => {
                  // The item already exists in the cache, evaluate preconditions based on its attributes and the headers
                  val lastMod = new Date(b.lastModified)
                  request.evaluatePreconditions(lastMod, calcETAG(b)) match {
                     // One of the preconditions failed, build a response
                     case bldr: ResponseBuilder => bldr.build
                     // Preconditions passed
                     case null => putInCache(cache, mediaType, key, data, ttl, idleTime)
                  }
               }
               case null => putInCache(cache, mediaType, key, data, ttl, idleTime)
            }
         }
      }
   }
      
   private def putInCache(cache: Cache[String, Any], mediaType: String, key: String, data: Array[Byte], ttl: Long, idleTime: Long): Response = {
      val obj = if (isBinaryType(mediaType)) data else new MIMECacheEntry(mediaType, data)
      (ttl, idleTime, useAsync) match {
         case (0, 0, false) => cache.put(key, obj)
         case (x, 0, false) => cache.put(key, obj, ttl, TimeUnit.SECONDS)
         case (x, y, false) => cache.put(key, obj, ttl, TimeUnit.SECONDS, idleTime, TimeUnit.SECONDS)
         case (0, 0, true) => cache.putAsync(key, obj)
         case (x, 0, true) => cache.putAsync(key, obj, ttl, TimeUnit.SECONDS)
         case (x, y, true) => cache.putAsync(key, obj, ttl, TimeUnit.SECONDS, idleTime, TimeUnit.SECONDS)
      }
      Response.ok.build
   }

   @DELETE
   @Path("/{cacheName}/{cacheKey}")
   def removeEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String): Response = {
      ManagerInstance.getEntry(cacheName, key) match {
         case b: MIMECacheEntry => {
            // The item exists in the cache, evaluate preconditions based on its attributes and the headers
	        val lastMod = new Date(b.lastModified)
	        request.evaluatePreconditions(lastMod, calcETAG(b)) match {
               // One of the preconditions failed, build a response
               case bldr: ResponseBuilder => bldr.build
               // Preconditions passed
               case null => {
                  if (useAsync) {
                     ManagerInstance.getCache(cacheName).removeAsync(key)
                  } else {
                     ManagerInstance.getCache(cacheName).remove(key)
                  }
                  Response.ok.build
               }
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
         ManagerInstance.getCache(cacheName).clear
         Response.ok.build
      } else {
         preconditionNotImplementedResponse
      }
   }
   
   private def preconditionNotImplementedResponse() = {
      Response.status(501).entity(
         "Preconditions were not implemented yet for PUT, POST, and DELETE methods.").build()
   }

   val hashFunc = new MurmurHash3()

   def calcETAG(entry: MIMECacheEntry) = new EntityTag(entry.contentType + hashFunc.hash(entry.data))

   private def protectCacheNotFound(request: Request, useAsync: Boolean) (op: (Request, Boolean) => Response): Response = {
      try {
         op(request, useAsync)
      } catch {
         case e: CacheNotFoundException => Response status (Status.NOT_FOUND) build
      }
   }

   private def isBinaryType(mediaType: String) =
      mediaType == "application/x-java-serialized-object"

}

/**
 * Just wrap a single instance of the Infinispan cache manager. 
 */
object ManagerInstance {
   var instance: EmbeddedCacheManager = null

   def getCache(name: String): Cache[String, Any] = {
      if (name != CacheContainer.DEFAULT_CACHE_NAME && !instance.getCacheNames.contains(name))
         throw new CacheNotFoundException("Cache with name '" + name + "' not found amongst the configured caches")

      if (name == CacheContainer.DEFAULT_CACHE_NAME) instance.getCache[String, Any]
      else instance.getCache(name)
   }

   def getEntry(cacheName: String, key: String): Any = getCache(cacheName).get(key)

}

class CacheNotFoundException(msg: String) extends CacheException(msg)

