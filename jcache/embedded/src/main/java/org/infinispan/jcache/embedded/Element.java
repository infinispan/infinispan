package org.infinispan.jcache.embedded;

enum Element {
   STORE("jcache-store"),
   WRITER("jcache-writer");

   final String name;

   Element(String name) {
      this.name = name;
   }

   @Override
   public String toString() {
      return name;
   }
}
