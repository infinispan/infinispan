package org.infinispan.cli.resources;

import java.io.IOException;
import java.util.Optional;

import org.aesh.command.shell.Shell;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.printers.PrettyPrinter;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface Resource {

   enum ListFormat {
      NAMES,
      VALUES,
      FULL
   }

   String THIS = ".";
   String PARENT = "..";

   /**
    * Returns the name of this resource
    */
   String getName();

   /**
    * Returns the parent resource of this resource. This is null if the resource represents the root.
    */
   Resource getParent();

   /**
    * Returns an iterable over the children of this resource
    */
   Iterable<String> getChildrenNames() throws IOException;

   void printChildren(ListFormat format, int limit, PrettyPrinter.PrettyPrintMode prettyPrintMode, Shell shell) throws IOException;

   /**
    * Returns a resource representing the named child
    */
   Resource getChild(String name) throws IOException;

   /**
    * Returns a resource representing the named child
    */
   Resource getChild(String... name) throws IOException;

   /**
    * Finds the resource of the specified type within the ancestors of this resource
    */
   <T extends Resource> T findAncestor(Class<T> resourceClass);

   /**
    * Finds the resource of the specified type within the ancestors of this resource
    */
   <T extends Resource> Optional<T> optionalFindAncestor(Class<T> resourceClass);

   /**
    * Returns whether this resource is a leaf resource (i.e. it has no children, or the children are not navigable)
    */
   boolean isLeaf();

   /**
    * Returns a textual representation of this resource
    */
   String describe() throws IOException;

   /**
    * Returns a root resource configured against the supplied configuration
    */
   static Resource getRootResource(Connection connection) {
      return new RootResource(connection);
   }

   Resource getResource(String path) throws IOException;
}
