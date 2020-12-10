package org.infinispan.rest.framework;

import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.context.Flag;

/**
 * @since 10.0
 */
public interface RestRequest {

   Method method();

   String path();

   String uri();

   ContentSource contents();

   Map<String, List<String>> parameters();

   String getParameter(String name);

   Map<String, String> variables();

   String getAction();

   MediaType contentType();

   MediaType keyContentType();

   String getAcceptHeader();

   String getAuthorizationHeader();

   String getCacheControlHeader();

   String getContentTypeHeader();

   String getEtagIfMatchHeader();

   String getIfModifiedSinceHeader();

   String getEtagIfNoneMatchHeader();

   String getIfUnmodifiedSinceHeader();

   Long getMaxIdleTimeSecondsHeader();

   Long getTimeToLiveSecondsHeader();

   EnumSet<AdminFlag> getAdminFlags();

   Flag[] getFlags();

   Long getCreatedHeader();

   Long getLastUsedHeader();

   Subject getSubject();

   void setSubject(Subject subject);

   void setVariables(Map<String, String> variables);

   void setAction(String action);

   String header(String name);

   List<String> headers(String name);

   InetSocketAddress getRemoteAddress();
}
