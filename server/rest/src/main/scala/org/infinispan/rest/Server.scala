package org.infinispan.rest

import remoting.MIMECacheEntry
import java.util.concurrent.TimeUnit
import javax.ws.rs._
import core._
import core.Response.{ResponseBuilder, Status}
import manager.{CacheManager, DefaultCacheManager}

@Path("/rest")
class Server(@Context request: Request, @HeaderParam("performAsync") useAsync: Boolean) {

  @GET
  @Path("/{cacheName}/{cacheKey}")
  def getEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String) = {
      ManagerInstance.getEntry(cacheName, key) match {
        case b: MIMECacheEntry => {
          request.evaluatePreconditions(b.lastModified, calcETAG(b)) match {
            case bldr: ResponseBuilder => bldr.build
            case null => Response.ok(b.data, b.contentType).lastModified(b.lastModified).tag(calcETAG(b)).build
          }
        }
        case null => Response status(Status.NOT_FOUND) build
      }
  }

  @HEAD
  @Path("/{cacheName}/{cacheKey}")
  def headEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String) = {
      ManagerInstance.getEntry(cacheName, key) match {
        case b: MIMECacheEntry => {
          request.evaluatePreconditions(b.lastModified, calcETAG(b)) match {
            case bldr: ResponseBuilder => bldr.build
            case null => Response.ok.`type`(b.contentType).lastModified(b.lastModified).tag(calcETAG(b)).build
          }
        }
        case null => Response status(Status.NOT_FOUND) build
      }
  }


  @PUT
  @POST
  @Path("/{cacheName}/{cacheKey}")
  def putEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String,
            @HeaderParam("Content-Type") mediaType: String, data: Array[Byte],
            @HeaderParam("timeToLiveSeconds") ttl: Long,
            @HeaderParam("maxIdleTimeSeconds") idleTime: Long) = {
            val cache = ManagerInstance.getCache(cacheName)
            if (request.getMethod == "POST" && cache.containsKey(key)) {
                Response.status(Status.CONFLICT).build()
            } else {
              (ttl, idleTime, useAsync) match {
                case (0, 0, false) => cache.put(key, new MIMECacheEntry(mediaType, data))
                case (x, 0, false) => cache.put(key, new MIMECacheEntry(mediaType, data), ttl, TimeUnit.SECONDS)
                case (x, y, false) => cache.put(key, new MIMECacheEntry(mediaType, data), ttl, TimeUnit.SECONDS, idleTime, TimeUnit.SECONDS)
                case (0, 0, true) => cache.putAsync(key, new MIMECacheEntry(mediaType, data))
                case (x, 0, true) => cache.putAsync(key, new MIMECacheEntry(mediaType, data), ttl, TimeUnit.SECONDS)
                case (x, y, true) => cache.putAsync(key, new MIMECacheEntry(mediaType, data), ttl, TimeUnit.SECONDS, idleTime, TimeUnit.SECONDS)
              }
              Response.ok.build
            }

  }

  @DELETE
  @Path("/{cacheName}/{cacheKey}")
  def removeEntry(@PathParam("cacheName") cacheName: String, @PathParam("cacheKey") key: String) = {
    if (useAsync) {
      ManagerInstance.getCache(cacheName).removeAsync(key)      
    } else {
      ManagerInstance.getCache(cacheName).remove(key)
    }
  }

  @DELETE
  @Path("/{cacheName}")
  def killCache(@PathParam("cacheName") cacheName: String) = {
    ManagerInstance.getCache(cacheName).stop
  }

  def calcETAG(entry: MIMECacheEntry) = {
    new EntityTag(entry.contentType + entry.lastModified.getTime  + entry.data.length)

  }

}

/**
 * Just wrap a single instance of the Infinispan cache manager. 
 */
object ManagerInstance {
   var instance: CacheManager = null
   def getCache(name: String) = {
      instance.getCache(name).asInstanceOf[Cache[String, MIMECacheEntry]]
   }
   def getEntry(cacheName: String, key: String) : MIMECacheEntry = {
     getCache(cacheName).get(key)
   }
}

