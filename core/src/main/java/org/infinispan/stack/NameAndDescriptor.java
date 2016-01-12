package org.infinispan.stack;

/**
 * Method name and its descriptor
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class NameAndDescriptor {
   public static final NameAndDescriptor DUMMY = new NameAndDescriptor(null, null, null);
   private final String className;
   private final String name;
   private final String descriptor;

   public NameAndDescriptor(String className, String name, String descriptor) {
      this.className = className;
      this.name = name;
      this.descriptor = descriptor;
   }

   public String getClassName() {
      return className;
   }

   public String getName() {
      return name;
   }

   public String getDescriptor() {
      return descriptor;
   }
}
