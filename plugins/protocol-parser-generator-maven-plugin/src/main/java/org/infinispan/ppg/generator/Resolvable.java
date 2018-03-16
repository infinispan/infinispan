package org.infinispan.ppg.generator;

import java.util.List;

public interface Resolvable {
   String qualifiedName();

   String sourceName();

   Machine.State addStates(Machine.State prev, Machine.State target, Machine machine, Grammar grammar, List<RuleDefinition> ruleStack, Reference reference);

   String analyzeType(Grammar grammar);
}
