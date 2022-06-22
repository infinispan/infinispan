package org.infinispan.cli.resources;

import static org.infinispan.cli.util.TransformingIterable.SINGLETON_MAP_VALUE;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.aesh.command.shell.Shell;
import org.infinispan.cli.logging.Messages;
import org.infinispan.cli.printers.CacheEntryRowPrinter;
import org.infinispan.cli.printers.PrettyPrinter;
import org.infinispan.cli.printers.PrettyRowPrinter;
import org.infinispan.cli.util.TransformingIterable;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CacheResource extends AbstractResource {
   public CacheResource(CachesResource parent, String name) {
      super(parent, name);
   }

   public Iterable<String> getChildrenNames(int limit) throws IOException {
      Iterable<Map<String, String>> keys = getConnection().getCacheKeys(getParent().getParent().getName(), name, limit);
      return new TransformingIterable<>(keys, SINGLETON_MAP_VALUE);
   }

   @Override
   public void printChildren(ListFormat format, int limit, PrettyPrinter.PrettyPrintMode prettyPrintMode, Shell shell) throws IOException {
      Iterable<Map<String, String>> it;
      PrettyRowPrinter rowPrinter;
      switch (format) {
         case NAMES:
            it = getConnection().getCacheKeys(getParent().getParent().getName(), name, limit);
            rowPrinter = new CacheEntryRowPrinter(shell.size().getWidth(), 1);
            break;
         case VALUES:
            it = getConnection().getCacheEntries(getParent().getParent().getName(), name, limit, false);
            rowPrinter = new CacheEntryRowPrinter(shell.size().getWidth(), 2);
            break;
         case FULL:
            it = getConnection().getCacheEntries(getParent().getParent().getName(), name, limit, true);
            rowPrinter = new CacheEntryRowPrinter(shell.size().getWidth(), 7);
            break;
         default:
            throw Messages.MSG.unsupportedListFormat(format);
      }

      try(PrettyPrinter printer = PrettyPrinter.forMode(prettyPrintMode, shell, rowPrinter)) {
         printer.print(it);
      }
   }

   @Override
   public boolean isLeaf() {
      return true;
   }

   @Override
   public Resource getChild(String name) {
      if (Resource.PARENT.equals(name)) {
         return parent;
      } else {
         return new CacheKeyResource(this, name);
      }
   }

   @Override
   public String describe() throws IOException {
      return getConnection().describeCache(getParent().getParent().getName(), name);
   }

   public static String cacheName(Resource resource) {
      return resource.findAncestor(CacheResource.class).getName();
   }

   public static Optional<String> findCacheName(Resource resource) {
      return resource.optionalFindAncestor(CacheResource.class).map(AbstractResource::getName);
   }
}
