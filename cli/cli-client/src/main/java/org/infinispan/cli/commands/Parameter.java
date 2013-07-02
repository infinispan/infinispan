package org.infinispan.cli.commands;

public class Parameter implements Argument {
   final String value;
   final int offset;

   public Parameter(String value, int offset) {
      this.value = value;
      this.offset = offset;
   }

   @Override
   public int getOffset() {
      return offset;
   }

   @Override
   public String getValue() {
      return value;
   }
}
