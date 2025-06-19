package org.infinispan.rest.framework;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.rest.framework.openapi.Parameter;
import org.infinispan.rest.framework.openapi.RequestBody;
import org.infinispan.rest.framework.openapi.ResponseContent;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Defines an invocation to a REST resource.
 *
 * @since 10.0
 */
public interface Invocation {

   /**
    * Returns one or more methods supported.
    */
   Set<Method> methods();

   /**
    * Returns the associated action (request parameter) or null.
    */
   String action();

   /**
    * Returns one or more paths associated with the invocation.
    * Paths can be constant, e.g. /a/b/c or use variables such as /a/{var1}/{var2}.
    */
   Set<String> paths();

   /**
    * The user-friendly name of the invocation
    */
   default String name() {
      return toString();
   }

   /**
    * Return the function to execute the invocation.
    */
   Function<RestRequest, CompletionStage<RestResponse>> handler();

   /**
    * @return true whether the invocation can be done anonymously (without auth)
    */
   boolean anonymous();

   /**
    * @return true if the invocation is deprecated
    */
   boolean deprecated();

   /**
    * @return the required permission for this invocation when authorization is enabled
    */
   AuthorizationPermission permission();

   AuditContext auditContext();

   /**
    * Verify whether the invocation requires the cache manager to be started.
    *
    * @return <code>true</code> means the cache manager must be running. <code>false</code>, otherwise.
    */
   boolean requireCacheManagerStart();

   /**
    * @return The resource group which contains this invocation.
    */
   ResourceDescription resourceGroup();

   Collection<Parameter> parameters();

   RequestBody requestBody();

   Map<HttpResponseStatus, ResponseContent> responses();

   String operationId();
}
