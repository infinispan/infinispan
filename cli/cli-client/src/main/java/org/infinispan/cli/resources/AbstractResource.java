package org.infinispan.cli.resources;

import java.io.IOException;
import java.util.Collections;

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
      if (Resource.PARENT.equals(name)) {
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
      if (resourceClass.isAssignableFrom(this.getClass())) {
         return (T) this;
      } else if (parent != null) {
         return parent.findAncestor(resourceClass);
      } else {
         throw Messages.MSG.illegalContext();
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
}
