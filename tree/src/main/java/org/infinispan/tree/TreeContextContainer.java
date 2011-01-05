package org.infinispan.tree;

/**
 * Invocation context container holding tree invocation context for the
 * current thread.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class TreeContextContainer {

   ThreadLocal<TreeContext> tcTL = new ThreadLocal<TreeContext>();

   public TreeContext createTreeContext() {
      TreeContext existing = tcTL.get();
      if (existing == null) {
         TreeContext treeContext = new TreeContext();
         tcTL.set(treeContext);
         return treeContext;
      } else {
         return existing;
      }
   }

   public TreeContext getTreeContext() {
      return tcTL.get();
   }

   public TreeContext suspend() {
      TreeContext treeContext = tcTL.get();
      tcTL.remove();
      return treeContext;
   }
}
