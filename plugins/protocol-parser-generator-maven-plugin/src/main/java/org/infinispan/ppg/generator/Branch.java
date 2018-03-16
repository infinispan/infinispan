package org.infinispan.ppg.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class Branch {
   private final RuleDefinition rule;
   final List<Element> elements = new ArrayList<>();
   final String file;
   final int line;
   Action sentinel;

   Branch(RuleDefinition rule, String file, int line) {
      this.rule = rule;
      this.file = file;
      this.line = line;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      if (sentinel != null) {
         sb.append("{ ").append(sentinel).append(" }? ");
      }
      for (Element e : elements) {
         sb.append(e).append(" ");
      }
      return sb.toString();
   };

   public void addReferences(Set<Reference> references) {
      for (Element e : elements) {
         e.addReferences(references);
      }
   }

   public Machine.State addStates(Machine.State prev, Machine.State target, Machine machine, Grammar grammar, List<RuleDefinition> ruleStack) {
      if (sentinel != null) {
         prev = prev.addSentinel(sentinel.code(grammar), machine.addState(ruleStack));
      }
      for (int i = 0; i < elements.size() - 1; i++) {
         Element e = elements.get(i);
         prev = e.addStates(prev, machine.addState(ruleStack), machine, grammar, ruleStack);
      }
      Element last = elements.get(elements.size() - 1);
      if (last instanceof Reference && elements.stream().filter(Reference.class::isInstance).count() == 1) {
         prev = last.addStates(prev, machine.addState(ruleStack), machine, grammar, ruleStack);
         return prev.addBacktrack(rule.sourceName + " = " + ((Reference) last).sourceName + ";", target);
      } else if (last instanceof Action) {
         String type = last.analyzeType(grammar);
         if ("throw".equals(type)) {
            last.addStates(prev, null, machine, grammar, ruleStack);
            return target;
         }
         if (!"void".equals(type) || rule.explicitType != null) {
            return prev.addBacktrack(rule.sourceName + " = " + ((Action) last).code(grammar) + ";", target);
         }
      }
      return last.addStates(prev, target, machine, grammar, ruleStack);
   }

   public String analyzeType(Grammar grammar) {
      Element last = elements.get(elements.size() - 1);
      if (last instanceof Reference && elements.stream().filter(Reference.class::isInstance).count() == 1) {
         return last.analyzeType(grammar);
      } else if (last instanceof Action) {
         return last.analyzeType(grammar);
      }
      return "void";
   }
}
