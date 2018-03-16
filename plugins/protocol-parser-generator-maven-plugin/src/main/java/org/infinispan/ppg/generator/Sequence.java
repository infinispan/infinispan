package org.infinispan.ppg.generator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Several elements, useful when these repeat
 */
public class Sequence implements Element {
   final List<Element> elements = new ArrayList<>();

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("( ");
      for (Element e : elements) {
         sb.append(e).append(" ");
      }
      return sb.append(')').toString();
   };

   @Override
   public void addReferences(Set<Reference> references) {
      for (Element e : elements) {
         e.addReferences(references);
      }
   }

   @Override
   public Machine.State addStates(Machine.State prev, Machine.State target, Machine machine, Grammar grammar, List<RuleDefinition> ruleStack) {
      for (Iterator<Element> iterator = elements.iterator(); iterator.hasNext(); ) {
         Element e = iterator.next();
         Machine.State next = iterator.hasNext() ? machine.addState(ruleStack) : target;
         prev = e.addStates(prev, next, machine, grammar, ruleStack);
      }
      assert prev == target; // could fail if sequence is empty but that's illegal
      return prev;
   }

   @Override
   public String analyzeType(Grammar grammar) {
      return elements.get(elements.size() - 1).analyzeType(grammar);
   }
}
