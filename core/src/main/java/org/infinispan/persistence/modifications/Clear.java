package org.infinispan.persistence.modifications;

public class Clear implements Modification {
   @Override
   public Type getType() {
      return Type.CLEAR;
   }
}
