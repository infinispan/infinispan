package org.infinispan.ppg.generator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class Action implements Element {
   private static final Set<String> DOUBLE_TOKEN_OPERATORS = new HashSet<>(Arrays.asList(
         "&&", "||", "!=", "==", "+=", "-=", "*=", "/=", "++", "--", "<<", ">>", "<=", ">=", "->"));

   private final String ns;
   private final List<String> action;
   private final String file;
   private final int line;

   Action(String ns, List<String> action, String file, int line) {
      this.ns = ns;
      this.action = action;
      this.file = file;
      this.line = line;
   }

   @Override
   public String toString() {
      return "{ " + action + "}";
   }

   @Override
   public void addReferences(Set<Reference> references) {
   }

   @Override
   public Machine.State addStates(Machine.State prev, Machine.State target, Machine machine, Grammar grammar, List<RuleDefinition> ruleStack) {
      String code = code(grammar);
      if (!code.endsWith(";\n")) {
         code = code + ";\n";
      }
      return prev.addAction(code, target);
   }

   @Override
   public String analyzeType(Grammar grammar) {
      if (action.size() >= 1 && "throw".equals(action.get(0))) {
         return "throw";
      } else if (action.size() >= 2 && "new".equals(action.get(0))) {
         String array = "";
         if (action.size() > 2 && "[".equals(action.get(2))) {
            array = "[";
         }
         return action.get(1) + array;
      }

      if (action.size() == 1) {
         String token = action.get(0);
         if (token.equals("true") || token.equals("false")) {
            return "boolean";
         } else if (token.matches("[0-9]*[Ll]")) {
            return "long";
         } else if (token.matches("[0-9]*")) {
            return "int";
         }
      }
      return "void";
   }

   public String code(Grammar grammar) {
      StringBuilder sb = new StringBuilder();
      boolean requiresSpace = false;
      for (int i = 0; i < action.size(); i++) {
         String item = action.get(i);
         String lookeahead = (i + 1) < action.size() ? action.get(i + 1) : "";
         assert !item.isEmpty();
         char firstChar = item.charAt(0);
         if (firstChar == ';') {
            sb.append(item).append('\n');
            requiresSpace = false;
            continue;
         } else if (firstChar == ')' || firstChar == ']' || firstChar == ',') {
            sb.append(item);
            requiresSpace = true;
            continue;
         } else if (firstChar == '(' || firstChar == '[' || firstChar == '.') {
            sb.append(item);
            requiresSpace = false;
            continue;
         }
         if (requiresSpace) {
            sb.append(' ');
         }
         if (Character.isLetter(firstChar) || firstChar == '@') {
            requiresSpace = true;
            // TODO omit java keywords?
            Resolvable resolvable = grammar.qualified.get(item);
            if (resolvable != null) {
               sb.append(resolvable.sourceName());
               continue;
            }
            resolvable = grammar.qualified.get(ns + "." + item);
            if (resolvable != null) {
               sb.append(resolvable.sourceName());
               continue;
            }
            int dotIndex = item.indexOf('.');
            if (dotIndex > 0) {
               String varName = item.substring(0, dotIndex);
               resolvable = grammar.qualified.get(ns + "." + varName);
               if (resolvable != null) {
                  sb.append(resolvable.sourceName()).append('.').append(item.substring(dotIndex + 1));
                  continue;
               }
            }
         }
         sb.append(item);
         if ("if".equals(item) || "while".equals(item)) {
            // '(' won't print space before so we do it explicitly
            sb.append(' ');
         } else if (firstChar == '@' || firstChar == '{' || firstChar == '}' && !lookeahead.equals("else")) {
            // TODO: this will cause problems if the characters are a part of string
            sb.append('\n');
            requiresSpace = false;
         } else if (DOUBLE_TOKEN_OPERATORS.contains(item + lookeahead)) {
            requiresSpace = false;
         } else {
            requiresSpace = true;
         }
      }
      return sb.toString();
   }
}
