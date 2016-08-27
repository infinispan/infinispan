package org.infinispan.cli;

public interface Config {
   boolean isColorEnabled();

   boolean isHistoryEnabled();

   String getPrompt();

   void load();

   void save();

}
