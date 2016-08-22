package org.infinispan.rest;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Variant;

import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.AdvancedCache;
import org.infinispan.CacheSet;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.jboss.resteasy.util.HttpHeaderNames;

import com.thoughtworks.xstream.XStream;

/**
 * Integration server linking REST requests with Infinispan calls.
 *
 * @author Michael Neale
 * @author Galder Zamarreño
 * @since 4.0
 */
@Path("/rest")
public class Server {
   private final RestServerConfiguration configuration;
   private final RestCacheManager manager;

   private final MurmurHash3 hashFunc = MurmurHash3.getInstance();

   private final static MediaType TextPlainUtf8Type = new MediaType("text", "plain", "UTF-8");
   private final static String TextPlainUtf8 = TextPlainUtf8Type.toString();
   private final static MediaType ApplicationXJavaSerializedObjectType = new MediaType("application", "x-java-serialized-object");
   private final static String ApplicationXJavaSerializedObject = ApplicationXJavaSerializedObjectType.toString();
   private final static String TimeToLiveHeader = "timeToLiveSeconds";
   private final static String MaxIdleTimeHeader = "maxIdleTimeSeconds";

   /**
    * For dealing with binary entries in the cache
    */
   private static class VariantListHelper {
      public static final List<Variant> variantList = Variant.VariantListBuilder.newInstance().mediaTypes(
            MediaType.APPLICATION_XML_TYPE, ApplicationXJavaSerializedObjectType, MediaType.APPLICATION_JSON_TYPE).build();
   }

   private static class CollectionVariantListHelper {
      public static final List<Variant> collectionVariantList = Variant.VariantListBuilder.newInstance().mediaTypes(
            MediaType.TEXT_HTML_TYPE,
            MediaType.APPLICATION_XML_TYPE,
            MediaType.APPLICATION_JSON_TYPE,
            MediaType.TEXT_PLAIN_TYPE,
            TextPlainUtf8Type
      ).build();
   }

   private static class JsonMapperHolder {
      public static final ObjectMapper jsonMapper = new ObjectMapper();
   }

   private static class XStreamholder {
      public static final XStream XStream = new XStream();
   }

   private static final DateFormat DatePatternRfc1123LocaleUS = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

   static {
      DatePatternRfc1123LocaleUS.setTimeZone(TimeZone.getTimeZone("GMT"));
   }

   public Server(RestServerConfiguration configuration, RestCacheManager manager) {
      this.configuration = configuration;
      this.manager = manager;
   }

   @GET
   @Path("/{cacheName}")
   public Response getKeys(@Context Request request, @HeaderParam("performAsync") boolean useAsync,
                           @PathParam("cacheName") String cacheName, @QueryParam("global") String globalKeySet) {
      return protectCacheNotFound(() -> {
         AdvancedCache<String, ?> cache = manager.getCache(cacheName);
         CacheSet<String> keys = cache.keySet();
         Variant variant = request.selectVariant(CollectionVariantListHelper.collectionVariantList);
         String selectedMediaType = variant != null ? variant.getMediaType().toString() : null;
         if (MediaType.TEXT_HTML.equals(selectedMediaType)) {
            return Response.ok().type(MediaType.TEXT_HTML).entity(printIt(pw -> {
               pw.print("<html><body>");
               keys.forEach(key -> {
                  String hkey = Escaper.escapeHtml(key);
                  pw.printf("<a href=\"%s/%s\">%s</a><br/>", cacheName, hkey, hkey);
               });
               pw.print("</body></html>");
            })).build();
         } else if (MediaType.APPLICATION_XML.equals(selectedMediaType)) {
            return Response.ok().type(MediaType.APPLICATION_XML).entity(printIt(pw -> {
               pw.print("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + System.lineSeparator() + System.lineSeparator() + "<keys>");
               keys.forEach(key -> pw.printf("<key>%s</key>", Escaper.escapeXml(key)));
               pw.print("</keys>");
            })).build();
         } else if (MediaType.APPLICATION_JSON.equals(selectedMediaType)) {
            return Response.ok().type(MediaType.APPLICATION_JSON).entity(printIt(pw -> {
               pw.print("keys=[");
               Iterator<String> it = keys.iterator();
               while (it.hasNext()) {
                  pw.printf("\"%s\"", Escaper.escapeJson(it.next()));
                  if (it.hasNext()) pw.print(",");

               }
               pw.print("]");
            })).build();
         } else if (MediaType.TEXT_PLAIN.equals(selectedMediaType)) {
            return Response.ok().type(MediaType.TEXT_PLAIN).entity(printIt(pw -> keys.forEach(pw::println))).build();
         } else if (Server.TextPlainUtf8.equals(selectedMediaType)) {
            return Response.ok().type(Server.TextPlainUtf8Type).entity(printItUTF8(writer -> {
               keys.forEach(key -> {
                  try {
                     writer.write(key);
                     writer.write(System.lineSeparator());
                  } catch (IOException e) {
                     throw new CacheException(e);
                  }
               });
            })).build();
         } else {
            return Response.notAcceptable(CollectionVariantListHelper.collectionVariantList).build();
         }
      });
   }

   @GET
   @Path("/{cacheName}/{cacheKey}")
   public <V> Response getEntry(@Context Request request, @HeaderParam("performAsync") boolean useAsync,
                                @PathParam("cacheName") String cacheName, @PathParam("cacheKey") String key,
                                @QueryParam("extended") String extended,
                                @DefaultValue("") @HeaderParam("Cache-Control") String cacheControl) {
      return protectCacheNotFound(() -> {
         CacheEntry<String, V> entry = manager.getInternalEntry(cacheName, key);
         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry<String, V> ice = (InternalCacheEntry<String, V>) entry;
            Date lastMod = lastModified(ice);
            Date expires = ice.canExpire() ? new Date(ice.getExpiryTime()) : null;
            OptionalInt minFreshSeconds = minFresh(cacheControl);
            return ensureFreshEnoughEntry(expires, minFreshSeconds, () -> {
               Metadata meta = ice.getMetadata();
               if (meta instanceof MimeMetadata) {
                  return getMimeEntry(request, ice, (MimeMetadata) meta, lastMod, expires, cacheName, extended);
               } else {
                  return getAnyEntry(request, ice, meta, lastMod, expires, cacheName, extended);
               }
            });
         } else {
            return Response.status(Response.Status.NOT_FOUND).build();
         }
      });
   }

   private Response ensureFreshEnoughEntry(Date expires, OptionalInt minFreshSeconds, Supplier<Response> supplier) {
      if (entryFreshEnough(expires, minFreshSeconds)) {
         return supplier.get();
      } else {
         return Response.status(Response.Status.NOT_FOUND).build();
      }
   }

   private OptionalInt minFresh(String cacheControl) {
      Optional<String> minFreshDirective = Arrays.stream(cacheControl.split(",")).filter(s -> s.contains("min-fresh")).findFirst();
      return minFreshDirective.map(s -> {
         String[] equals = s.split("=");
         return OptionalInt.of(Integer.parseInt(equals[equals.length - 1].trim()));
      }).orElse(OptionalInt.empty());
   }

   private boolean entryFreshEnough(Date entryExpires, OptionalInt minFresh) {
      return !minFresh.isPresent() || minFresh.getAsInt() < calcFreshness(entryExpires);
   }

   private int calcFreshness(Date expires) {
      if (expires == null) {
         return Integer.MAX_VALUE;
      } else {
         return ((int) (expires.getTime() - new Date().getTime()) / 1000);
      }
   }

   private <V> Response getMimeEntry(Request request, InternalCacheEntry<String, V> ice, MimeMetadata meta,
                                     Date lastMod, Date expires, String cacheName, String extended) {
      String key = ice.getKey();
      Response.ResponseBuilder bldr = request.evaluatePreconditions(lastMod, calcETAG(ice, meta));
      if (bldr == null) {
         bldr = extended(mortality(Response.ok(ice.getValue(), meta.contentType)
                     .header(HttpHeaderNames.LAST_MODIFIED, formatDate(lastMod))
                     //workaround for https://issues.jboss.org/browse/RESTEASY-887
                     .header(HttpHeaderNames.EXPIRES, formatDate(expires))
                     .cacheControl(calcCacheControl(expires)),
               meta).tag(calcETAG(ice, meta)),
               cacheName, key, wantExtendedHeaders(extended));
      }
      return bldr.build();
   }

   private <V> Response getAnyEntry(Request request, InternalCacheEntry<String, V> ice, Metadata meta,
                                    Date lastMod, Date expires, String cacheName, String extended) {
      String key = ice.getKey();
      V value = ice.getValue();
      if (value instanceof String) {
         return mortality(Response.ok((String) value, MediaType.TEXT_PLAIN)
                     .header(HttpHeaderNames.LAST_MODIFIED, formatDate(lastMod))
                     .cacheControl(calcCacheControl(expires))
                     .header(HttpHeaderNames.EXPIRES, formatDate(expires)),
               meta).build();
      } else if (value instanceof byte[]) {
         return extended(mortality(Response.ok()
                     .type(MediaType.APPLICATION_OCTET_STREAM)
                     .header(HttpHeaderNames.LAST_MODIFIED, formatDate(lastMod))
                     .header(HttpHeaderNames.EXPIRES, formatDate(expires))
                     .cacheControl(calcCacheControl(expires)),
               meta), cacheName, key, wantExtendedHeaders(extended))
               .entity(streamIt(b -> {
                  try {
                     b.write((byte[]) value);
                  } catch (IOException e) {
                     throw new CacheException(e);
                  }
               }))
               .build();
      } else {
         Variant variant = request.selectVariant(VariantListHelper.variantList);
         String selectedMediaType = variant != null ? variant.getMediaType().toString() : null;

         // For objects other than String or byte arrays, accept only JSON, XML and X_JAVA_SERIALIZABLE_OBJECT
         if (MediaType.APPLICATION_JSON.equals(selectedMediaType)) {
            return extended(mortality(Response.ok()
                        .type(selectedMediaType)
                        .header(HttpHeaderNames.LAST_MODIFIED, formatDate(lastMod))
                        .header(HttpHeaderNames.EXPIRES, formatDate(expires))
                        .cacheControl(calcCacheControl(expires)),
                  meta), cacheName, key, wantExtendedHeaders(extended))
                  .entity(streamIt(b -> {
                     try {
                        JsonMapperHolder.jsonMapper.writeValue(b, value);
                     } catch (IOException e) {
                        throw new CacheException(e);
                     }
                  }))
                  .build();
         } else if (MediaType.APPLICATION_XML.equals(selectedMediaType)) {
            return extended(mortality(Response.ok()
                        .type(selectedMediaType)
                        .header(HttpHeaderNames.LAST_MODIFIED, formatDate(lastMod))
                        .header(HttpHeaderNames.EXPIRES, formatDate(expires))
                        .cacheControl(calcCacheControl(expires)),
                  meta), cacheName, key, wantExtendedHeaders(extended))
                  .entity(streamIt(b -> XStreamholder.XStream.toXML(value, b)))
                  .build();
         } else if (Server.ApplicationXJavaSerializedObject.equals(selectedMediaType)) {
            if (value instanceof Serializable) {
               return extended(mortality(Response.ok()
                           .type(selectedMediaType)
                           .header(HttpHeaderNames.LAST_MODIFIED, formatDate(lastMod))
                           .header(HttpHeaderNames.EXPIRES, formatDate(expires))
                           .cacheControl(calcCacheControl(expires)),
                     meta), cacheName, key, wantExtendedHeaders(extended))
                     .entity(streamIt(b -> {
                        try {
                           new ObjectOutputStream(b).writeObject(value);
                        } catch (IOException e) {
                           throw new CacheException(e);
                        }
                     }))
                     .build();
            }
         }
         return Response.notAcceptable(VariantListHelper.variantList).build();
      }
   }

   private String formatDate(Date date) {
      if (date == null)
         return null;
      else
         return Server.DatePatternRfc1123LocaleUS.format(date);
   }

   private CacheControl calcCacheControl(Date expires) {
      if (expires == null) {
         return null;
      } else {
         CacheControl cacheControl = new CacheControl();
         int maxAgeSeconds = calcFreshness(expires);
         if (maxAgeSeconds > 0)
            cacheControl.setMaxAge(maxAgeSeconds);
         else
            cacheControl.setNoCache(true);
         return cacheControl;
      }
   }

   static Response.ResponseBuilder mortality(Response.ResponseBuilder bld, Metadata meta) {
      if (meta.lifespan() > -1)
         bld.header(Server.TimeToLiveHeader, TimeUnit.MILLISECONDS.toSeconds(meta.lifespan()));
      if (meta.maxIdle() > -1)
         bld.header(Server.MaxIdleTimeHeader, TimeUnit.MILLISECONDS.toSeconds(meta.maxIdle()));
      return bld;
   }

   Response.ResponseBuilder extended(Response.ResponseBuilder bld, String cacheName, String key, boolean b) {
      return b ? bld
            .header("Cluster-Primary-Owner", manager.getPrimaryOwner(cacheName, key))
            .header("Cluster-Node-Name", manager.getNodeName())
            .header("Cluster-Server-Address", manager.getServerAddress()) : bld;
   }


   private boolean wantExtendedHeaders(String extended) {
      switch (configuration.extendedHeaders()) {
         case NEVER:
            return false;
         case ON_DEMAND:
            return extended != null;
         default:
            throw new IllegalArgumentException("Unsupported header:" + configuration.extendedHeaders());
      }
   }

   /**
    * create a JAX-RS streaming output
    */
   StreamingOutput streamIt(Consumer<? super OutputStream> action) {
      return new StreamingOutput() {
         @Override
         public void write(OutputStream o) {
            action.accept(o);
         }
      };
   }

   StreamingOutput printIt(Consumer<? super PrintWriter> consumer) {
      return new StreamingOutput() {
         @Override
         public void write(OutputStream o) throws IOException, WebApplicationException {
            PrintWriter pw = new PrintWriter(o);
            try {
               consumer.accept(pw);
            } finally {
               pw.flush();
            }
         }
      };
   }

   StreamingOutput printItUTF8(Consumer<? super Writer> action) {
      return new StreamingOutput() {
         @Override
         public void write(OutputStream outputStream) throws IOException, WebApplicationException {
            Writer writer = new OutputStreamWriter(outputStream, "UTF-8");
            try {
               action.accept(writer);
            } finally {
               writer.flush();
            }
         }
      };
   }

   @HEAD
   @Path("/{cacheName}/{cacheKey}")
   public <V> Response headEntry(@Context Request request, @HeaderParam("performAsync") boolean useAsync,
                                 @PathParam("cacheName") String cacheName, @PathParam("cacheKey") String key,
                                 @QueryParam("extended") String extended,
                                 @DefaultValue("") @HeaderParam("Cache-Control") String cacheControl) {
      return protectCacheNotFound(() -> {
         CacheEntry<String, V> entry = manager.getInternalEntry(cacheName, key);
         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry<String, V> ice = (InternalCacheEntry<String, V>) entry;
            Date lastMod = lastModified(ice);
            Date expires = ice.canExpire() ? new Date(ice.getExpiryTime()) : null;
            OptionalInt minFreshSeconds = minFresh(cacheControl);
            return ensureFreshEnoughEntry(expires, minFreshSeconds, () -> {
               Metadata meta = ice.getMetadata();
               if (meta instanceof MimeMetadata) {
                  MimeMetadata mime = (MimeMetadata) meta;
                  Response.ResponseBuilder bldr = request.evaluatePreconditions(lastMod, calcETAG(ice, mime));
                  if (bldr == null) {
                     return extended(mortality(Response.ok()
                                 .type(mime.contentType)
                                 .header(HttpHeaderNames.LAST_MODIFIED, formatDate(lastMod))
                                 .header(HttpHeaderNames.EXPIRES, formatDate(expires))
                                 .cacheControl(calcCacheControl(expires)),
                           mime)
                                 .tag(calcETAG(ice, mime)),
                           cacheName, key, wantExtendedHeaders(extended))
                           .build();
                  } else {
                     return bldr.build();
                  }
               } else {
                  return extended(mortality(Response.ok()
                              .header(HttpHeaderNames.LAST_MODIFIED, formatDate(lastMod))
                              .header(HttpHeaderNames.EXPIRES, formatDate(expires))
                              .cacheControl(calcCacheControl(expires)),
                        meta), cacheName, key, wantExtendedHeaders(extended))
                        .build();
               }
            });
         } else {
            return Response.status(Response.Status.NOT_FOUND).build();
         }
      });
   }

   @PUT
   @POST
   @Path("/{cacheName}/{cacheKey}")
   public <V> Response putEntry(@Context Request request, @HeaderParam("performAsync") boolean useAsync,
                                @PathParam("cacheName") String cacheName, @PathParam("cacheKey") String key,
                                @HeaderParam("Content-Type") String mediaType, byte[] data,
                                @DefaultValue("-1") @HeaderParam("timeToLiveSeconds") long ttl,
                                @DefaultValue("-1") @HeaderParam("maxIdleTimeSeconds") long idleTime) {
      return protectCacheNotFound(() -> {
         AdvancedCache<String, byte[]> cache = manager.getCache(cacheName);
         if ("POST".equals(request.getMethod()) && cache.containsKey(key)) {
            return Response.status(Response.Status.CONFLICT).build();
         } else {
            CacheEntry<String, V> entry = manager.getInternalEntry(cacheName, key, true);
            if (entry instanceof InternalCacheEntry) {
               InternalCacheEntry ice = (InternalCacheEntry) entry;
               Date lastMod = lastModified(ice);
               Metadata meta = ice.getMetadata();
               if (meta instanceof MimeMetadata) {
                  // The item already exists in the cache, evaluate preconditions based on its attributes and the headers
                  EntityTag etag = calcETAG(ice, (MimeMetadata) meta);
                  Response.ResponseBuilder bldr = request.evaluatePreconditions(lastMod, etag);
                  if (bldr == null) {
                     // Preconditions passed
                     return putInCache(useAsync, cache, key, data, mediaType, ttl, idleTime,
                           Optional.of((byte[]) ice.getValue()));
                  } else {
                     // One of the preconditions failed, build a response
                     return bldr.build();
                  }
               } else {
                  return putInCache(useAsync, cache, key, data, mediaType, ttl, idleTime, Optional.empty());
               }
            } else {
               return putInCache(useAsync, cache, key, data, mediaType, ttl, idleTime, Optional.empty());
            }
         }
      });
   }

   private Response putInCache(boolean useAsync, AdvancedCache<String, byte[]> cache, String key,
                               byte[] data, String dataType, long ttl, long idleTime, Optional<byte[]> prevCond) {
      if (useAsync)
         return asyncPutInCache(cache, key, data, dataType, ttl, idleTime);
      else
         return putOrReplace(cache, key, data, dataType, ttl, idleTime, prevCond);
   }

   Response asyncPutInCache(AdvancedCache<String, byte[]> cache,
                            String key, byte[] data, String dataType, long ttl, long idleTime) {
      Metadata metadata = createMetadata(cache.getCacheConfiguration(), dataType, ttl, idleTime);
      cache.putAsync(key, data, metadata);
      return Response.ok().build();
   }

   Metadata createMetadata(Configuration cfg, String dataType, long ttl, long idleTime) {
      MimeMetadataBuilder metadata = new MimeMetadataBuilder();
      metadata.contentType(dataType);
      if (ttl == 0) {
         metadata.lifespan(cfg.expiration().lifespan(), TimeUnit.MILLISECONDS);
      } else {
         metadata.lifespan(ttl, TimeUnit.SECONDS);
      }
      if (idleTime == 0) {
         metadata.maxIdle(cfg.expiration().maxIdle(), TimeUnit.MILLISECONDS);
      } else {
         metadata.maxIdle(idleTime, TimeUnit.SECONDS);
      }
      return metadata.build();
   }

   private Response putOrReplace(AdvancedCache<String, byte[]> cache,
                                 String key, byte[] data, String dataType,
                                 long ttl, long idleTime,
                                 Optional<byte[]> prevCond) {
      Metadata metadata = createMetadata(cache.getCacheConfiguration(), dataType, ttl, idleTime);
      if (prevCond.isPresent()) {
         boolean replaced = cache.replace(key, prevCond.get(), data, metadata);
         // If not replaced, simply send back that the precondition failed
         if (replaced) {
            return Response.ok().build();
         } else {
            return Response.status(HttpServletResponse.SC_PRECONDITION_FAILED).build();
         }
      } else {
         cache.put(key, data, metadata);
         return Response.ok().build();
      }
   }

   @DELETE
   @Path("/{cacheName}/{cacheKey}")
   public <V> Response removeEntry(@Context Request request, @HeaderParam("performAsync") boolean useAsync,
                                   @PathParam("cacheName") String cacheName, @PathParam("cacheKey") String key) {
      return protectCacheNotFound(() -> {
         CacheEntry<String, V> entry = manager.getInternalEntry(cacheName, key);
         if (entry instanceof InternalCacheEntry) {
            InternalCacheEntry ice = (InternalCacheEntry) entry;
            Date lastMod = lastModified((InternalCacheEntry) entry);
            Metadata meta = entry.getMetadata();
            if (meta instanceof MimeMetadata) {
               // The item exists in the cache, evaluate preconditions based on its attributes and the headers
               EntityTag etag = calcETAG(ice, (MimeMetadata) meta);
               Response.ResponseBuilder bldr = request.evaluatePreconditions(lastMod, etag);
               if (bldr == null) {
                  // Preconditions passed
                  if (useAsync) {
                     manager.getCache(cacheName).removeAsync(key);
                  } else {
                     manager.getCache(cacheName).remove(key);
                  }
                  return Response.ok().build();
               } else {
                  // One of the preconditions failed, build a response
                  return bldr.build();
               }
            } else {
               if (useAsync) {
                  manager.getCache(cacheName).removeAsync(key);
               } else {
                  manager.getCache(cacheName).remove(key);
               }
               return Response.ok().build();
            }
         } else if (entry == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
         } else {
            throw new IllegalArgumentException("Unsupported entry implementation: " + entry);
         }
      });
   }

   @DELETE
   @Path("/{cacheName}")
   public Response killCache(@PathParam("cacheName") String cacheName,
                             @DefaultValue("") @HeaderParam("If-Match") String ifMatch,
                             @DefaultValue("") @HeaderParam("If-None-Match") String ifNoneMatch,
                             @DefaultValue("") @HeaderParam("If-Modified-Since") String ifModifiedSince,
                             @DefaultValue("") @HeaderParam("If-Unmodified-Since") String ifUnmodifiedSince) {
      if (ifMatch.isEmpty() && ifNoneMatch.isEmpty() && ifModifiedSince.isEmpty() && ifUnmodifiedSince.isEmpty()) {
         manager.getCache(cacheName).clear();
         return Response.ok().build();
      } else {
         return preconditionNotImplementedResponse();
      }
   }

   private Response preconditionNotImplementedResponse() {
      return Response.status(501).entity(
            "Preconditions were not implemented yet for PUT, POST, and DELETE methods.").build();
   }

   private <K, V> EntityTag calcETAG(InternalCacheEntry<K, V> entry, MimeMetadata meta) {
      return new EntityTag(meta.contentType + hashFunc.hash(entry.getValue()));
   }

   private <K, V> Date lastModified(InternalCacheEntry<K, V> ice) {
      return new Date(ice.getCreated() / 1000 * 1000);
   }

   private Response protectCacheNotFound(Supplier<Response> op) {
      try {
         return op.get();
      } catch (CacheNotFoundException e) {
         return Response.status(Response.Status.NOT_FOUND).build();
      }
   }
}

class CacheNotFoundException extends CacheException {
   public CacheNotFoundException(String msg) {
      super(msg);
   }
}

class CacheUnavailableException extends CacheException {
   public CacheUnavailableException(String msg) {
      super(msg);
   }
}

class Escaper {
   static String escapeHtml(String html) {
      return escapeXml(html);
   }

   static String escapeXml(String xml) {
      StringBuilder sb = new StringBuilder();
      for (char c : xml.toCharArray()) {
         switch (c) {
            case '&':
               sb.append("&amp;");
               break;
            case '>':
               sb.append("&gt;");
               break;
            case '<':
               sb.append("&lt;");
               break;
            case '\"':
               sb.append("&quot;");
               break;
            case '\'':
               sb.append("&apos;");
               break;
            default:
               sb.append(c);
               break;
         }
      }
      return sb.toString();
   }

   static String escapeJson(String json) {
      return json.replaceAll("\"", "\\\\\"");
   }
}
