package org.infinispan.rest.framework.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.Method;
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

   private final static Log logger = LogFactory.getLog(ResourceManagerImpl.class, Log.class);

   private final ResourceNode resourceTree;

   public ResourceManagerImpl() {
      this.resourceTree = new ResourceNode(new StringPathItem("/"), null);
   }

   @Override
   public void registerResource(String context, ResourceHandler handler) throws RegistrationException {
      handler.getInvocations().forEach(invocation -> {
         Set<String> paths = invocation.paths();
         paths.forEach(path -> {
            validate(path);
            List<PathItem> p = Arrays.stream(path.split("/")).filter(s -> !s.isEmpty()).map(PathItem::fromString).collect(Collectors.toList());
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
      List<PathItem> pathItems = Arrays.stream(path.replaceAll("//+", "/").split("/"))
            .map(s -> s.isEmpty() ? "/" : s).map(QueryStringDecoder::decodeComponent).map(StringPathItem::new).collect(Collectors.toList());
      return resourceTree.find(method, pathItems, action);
   }

}
