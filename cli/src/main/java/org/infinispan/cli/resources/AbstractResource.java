package org.infinispan.cli.resources;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class AbstractResource implements Resource {
   final Resource parent;
   final String name;

   protected AbstractResource(Resource parent, String name) {
      this.parent = parent;
      this.name = name;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public Resource getParent() {
      return parent;
   }

   @Override
   public Iterable<String> getChildrenNames() throws IOException {
      return Collections.emptyList();
   }

   @Override
   public Resource getChild(String name) throws IOException {
      if (Resource.PARENT.equals(name) && parent != null) {
         return parent;
      } else {
         throw Messages.MSG.noSuchResource(name);
      }
   }

   @Override
   public Resource getChild(String... name) throws IOException {
      if (name.length == 1) {
         return getChild(name[0]);
      } else {
         String[] children = new String[name.length - 1];
         System.arraycopy(name, 1, children, 0, name.length - 1);
         return getChild(name[0]).getChild(children);
      }
   }

   @Override
   public <T extends Resource> T findAncestor(Class<T> resourceClass) {
      return optionalFindAncestor(resourceClass).orElseThrow(Messages.MSG::illegalContext);
   }

   @Override
   public <T extends Resource> Optional<T> optionalFindAncestor(Class<T> resourceClass) {
      if (resourceClass.isAssignableFrom(this.getClass())) {
         return Optional.of(resourceClass.cast(this));
      } else if (parent != null) {
         return parent.optionalFindAncestor(resourceClass);
      } else {
         return Optional.empty();
      }
   }

   @Override
   public boolean isLeaf() {
      return false;
   }

   @Override
   public String describe() throws IOException {
      return name;
   }

   Connection getConnection() {
      return findAncestor(RootResource.class).getConnection();
   }

   @Override
   public Resource getResource(String path) throws IOException {
      if (path == null || Resource.THIS.equals(path)) {
         return this;
      } else if (Resource.PARENT.equals(path)) {
         Resource parent = getParent();
         if (parent != null) {
            return parent;
         } else {
            throw Messages.MSG.illegalContext();
         }
      } else {
         String[] parts = path.split("/");
         if (parts.length == 0) {
            return findAncestor(RootResource.class);
         } else {
            Resource resource = this;
            for (String part : parts) {
               if (part.isEmpty()) {
                  resource = resource.findAncestor(RootResource.class);
               } else {
                  resource = resource.getChild(part);
               }
            }
            return resource;
         }
      }
   }
}
