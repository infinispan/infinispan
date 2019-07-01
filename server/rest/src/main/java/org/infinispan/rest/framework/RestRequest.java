package org.infinispan.rest.framework;

import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 10.0
 */
public interface RestRequest {

   Method method();

   String path();

   String uri();

   ContentSource contents();

   Map<String, List<String>> parameters();

   Map<String, String> variables();

   String getAction();

   MediaType contentType();

   MediaType keyContentType();

   String getAcceptHeader();

   String getAuthorizationHeader();

   String getCacheControlHeader();

   String getContentTypeHeader();

   String getEtagIfMatchHeader();

   String getEtagIfModifiedSinceHeader();

   String getEtagIfNoneMatchHeader();

   String getEtagIfUnmodifiedSinceHeader();

   Long getMaxIdleTimeSecondsHeader();

   Long getTimeToLiveSecondsHeader();

   Long getCreatedHeader();

   Long getLastUsedHeader();

   Subject getSubject();

   void setSubject(Subject principal);

   void setVariables(Map<String, String> variables);

   void setAction(String action);

   String header(String name);

   List<String> headers(String name);
}
