package org.infinispan.rest

import codehaus.jackson.map.ObjectMapper
import com.thoughtworks.xstream.XStream
import java.io._
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
        case s: String => Response.ok(s, "text/plain").build
        case obj: Any => {
           val variant = request.selectVariant(variantList)
           val selectedMediaType =  if (variant != null) variant.getMediaType.toString else "application/x-java-serialized-object"
           selectedMediaType match {
             case MediaType.APPLICATION_JSON =>  Response.ok.`type`(selectedMediaType).entity(streamIt(jsonMapper.writeValue(_, obj))).build
             case MediaType.APPLICATION_XML => Response.ok.`type`(selectedMediaType).entity(streamIt(xstream.toXML(obj, _))).build
             case _ =>
               obj match {
                 case ser: Serializable =>
                  Response.ok.`type`("application/x-java-serialized-object").entity(streamIt(new ObjectOutputStream(_).writeObject(ser))).build
                 case _ => Response.notAcceptable(variantList).build
               }

           }
        }
        case null => Response status(Status.NOT_FOUND) build
      }
  }

  /** create a JAX-RS streaming output */
  def streamIt(action: (OutputStream) => Unit) = new StreamingOutput { def write(o: OutputStream) = {action(o)}}



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
        case x: Any => Response.ok.build
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
              val obj = if (mediaType == "application/x-java-serialized-object")
                new ObjectInputStream(new ByteArrayInputStream(data)).readObject
                else new MIMECacheEntry(mediaType, data)
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


  /** For dealing with binary entries in the cache */
  lazy val variantList = Variant.VariantListBuilder.newInstance.mediaTypes(MediaType.APPLICATION_XML_TYPE, MediaType.APPLICATION_JSON_TYPE).build
  lazy val jsonMapper = new ObjectMapper
  lazy val xstream = new XStream

}

/**
 * Just wrap a single instance of the Infinispan cache manager. 
 */
object ManagerInstance {
   var instance: CacheManager = null
   def getCache(name: String) = {
      instance.getCache(name).asInstanceOf[Cache[String, Any]]
   }
   def getEntry(cacheName: String, key: String) : Any = {
     getCache(cacheName).get(key)
   }
}

