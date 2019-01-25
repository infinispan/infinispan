package org.infinispan.rest.framework;

import java.security.Principal;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 10.0
 */
public interface RestRequest {

   Method method();

   String path();

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

   boolean getPerformAsyncHeader();

   Long getTimeToLiveSecondsHeader();

   Long getCreatedHeader();

   Long getLastUsedHeader();

   Principal getPrincipal();

   void setPrincipal(Principal principal);

   void setVariables(Map<String, String> variables);

   void setAction(String action);

}
