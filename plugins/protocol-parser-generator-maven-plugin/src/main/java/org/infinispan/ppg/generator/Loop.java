package org.infinispan.ppg.generator;

import java.util.List;
import java.util.Set;

public class Loop implements Element {
   final Reference counter;
   final Element element;

   public Loop(Reference counter, Element element) {
      this.counter = counter;
      this.element = element;
   }

   @Override
   public void addReferences(Set<Reference> references) {
      references.add(counter);
      element.addReferences(references);
   }

   @Override
   public Machine.State addStates(Machine.State prev, Machine.State target, Machine machine, Grammar grammar, List<RuleDefinition> ruleStack) {
      prev.addSentinel(counter.sourceName + " == 0", target);
      Machine.State afterDecrement = prev.addAction(counter.sourceName + "--;", machine.addState(ruleStack));
      element.addStates(afterDecrement, prev, machine, grammar, ruleStack);
      return target;
   }

   @Override
   public String analyzeType(Grammar grammar) {
      // repetition is supposed to cause side-effects such as accumulation, not return value
      return "<repetition>";
   }
}
