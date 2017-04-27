package org.infinispan.server.core.admin;

import java.util.EnumSet;

/**
 * Flags which affect only administrative operations
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public enum AdminFlag {
   /**
    * If the operation affects configuration, make it persistent. If the server cannot honor this flag an error will
    * be returned
    */
   PERSISTENT("persistent");


   private final String value;

   AdminFlag(String value) {
      this.value = value;
   }

   public static EnumSet<AdminFlag> fromString(String s) {
      EnumSet<AdminFlag> flags = EnumSet.noneOf(AdminFlag.class);
      if (s != null) {
         for (String name : s.split(",")) {
            flags.add(AdminFlag.valueOf(name));
         }
      }
      return flags;
   }
}
