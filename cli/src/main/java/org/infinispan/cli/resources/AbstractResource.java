package org.infinispan.cli.resources;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

import org.aesh.command.shell.Shell;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.logging.Messages;
import org.infinispan.cli.printers.DefaultRowPrinter;
import org.infinispan.cli.printers.PrettyPrinter;
import org.infinispan.cli.printers.PrettyRowPrinter;

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

   public void printChildren(ListFormat format, int limit, PrettyPrinter.PrettyPrintMode prettyPrintMode, Shell shell) throws IOException {
      Iterator<String> it = getChildrenNames().iterator();
      PrettyRowPrinter rowPrinter = new DefaultRowPrinter(shell.size().getWidth(), 1);
      try(PrettyPrinter printer = PrettyPrinter.forMode(prettyPrintMode, shell, rowPrinter)) {
         printer.print(it);
      }
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
