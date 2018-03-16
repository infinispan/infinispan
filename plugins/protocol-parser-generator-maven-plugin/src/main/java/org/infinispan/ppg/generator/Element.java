package org.infinispan.ppg.generator;

import java.util.List;
import java.util.Set;

public interface Element {
   void addReferences(Set<Reference> references);

   Machine.State addStates(Machine.State prev, Machine.State target, Machine machine, Grammar grammar, List<RuleDefinition> ruleStack);

   String analyzeType(Grammar grammar);
}
