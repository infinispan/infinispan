package org.infinispan.rest.framework.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.infinispan.rest.framework.InvocationRegistry;
import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.PathItem;
import org.infinispan.rest.framework.RegistrationException;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.ResourceManager;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.handler.codec.http.QueryStringDecoder;

/**
 * @since 10.0
 */
public class ResourceManagerImpl implements ResourceManager {

   private static final Log logger = LogFactory.getLog(ResourceManagerImpl.class, Log.class);
   private static final StringPathItem ROOT = new StringPathItem("/");

   private final ResourceNode resourceTree;

   public ResourceManagerImpl() {
      this.resourceTree = new ResourceNode(ROOT, null);
   }

   @Override
   public void registerResource(String context, ResourceHandler handler) throws RegistrationException {
      handler.getInvocations().forEach(invocation -> {
         Set<String> paths = invocation.paths();
         paths.forEach(path -> {
            validate(path);
            List<PathItem> p = Arrays.stream(path.split("/")).filter(s -> !s.isEmpty()).map(ResourceManagerImpl::fromString).toList();
            List<PathItem> pathWithCtx = new ArrayList<>();
            pathWithCtx.add(new StringPathItem(context));
            pathWithCtx.addAll(p);
            resourceTree.insertPath(invocation, pathWithCtx);
         });
      });
   }

   private void validate(String path) {
      if (path.contains("*") && !path.endsWith("*")) {
         throw logger.invalidPath(path);
      }
   }

   @Override
   public LookupResult lookupResource(Method method, String path, String action) {
      List<PathItem> pathItems = new ArrayList<>();
      final int l = path.length();
      int s = 0, pos = 0;
      while (pos < l) {
         char ch = path.charAt(pos);
         if (ch == '/') {
            if (pos == 0) {
               pathItems.add(ROOT);
            } else if (pos > s) {
               pathItems.add(new StringPathItem(QueryStringDecoder.decodeComponent(path.substring(s, pos))));
            }
            s = ++pos;
         } else {
            ++pos;
         }
      }
      if (s < pos) {
         pathItems.add(new StringPathItem(QueryStringDecoder.decodeComponent(path.substring(s))));
      }
      return resourceTree.find(method, pathItems, action);
   }

   @Override
   public InvocationRegistry registry() {
      return resourceTree;
   }

   private static PathItem fromString(String path) {
      if (PathItem.hasPathParameter(path)) return new VariablePathItem(path);
      return new StringPathItem(path);
   }
}
