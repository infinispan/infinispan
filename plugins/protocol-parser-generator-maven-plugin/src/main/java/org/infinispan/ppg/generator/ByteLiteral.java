package org.infinispan.ppg.generator;

import java.util.List;
import java.util.Set;

class ByteLiteral implements Element {
   private final int value;

   public ByteLiteral(int value) {
      this.value = value;
   }

   @Override
   public String toString() {
      return String.format("0x%02X", value);
   }

   @Override
   public void addReferences(Set<Reference> references) {
   }

   @Override
   public Machine.State addStates(Machine.State prev, Machine.State target, Machine machine, Grammar grammar, List<RuleDefinition> ruleStack) {
      return prev.requireReadByte(String.valueOf(value), target);
   }

   @Override
   public String analyzeType(Grammar grammar) {
      return "byte";
   }
}
