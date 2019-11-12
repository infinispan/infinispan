package org.infinispan.cli.commands;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.man.AeshFileDisplayer;
import org.aesh.command.man.FileParser;
import org.aesh.command.man.TerminalPage;
import org.aesh.command.man.parser.ManFileParser;
import org.aesh.command.option.Arguments;
import org.aesh.command.settings.ManProvider;
import org.aesh.terminal.utils.ANSI;
import org.aesh.terminal.utils.Config;
import org.infinispan.cli.completers.HelpCompleter;
import org.infinispan.cli.impl.CliManProvider;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
@CommandDefinition(name = Help.CMD, description = "Displays help for commands")
public class Help extends AeshFileDisplayer {

   public static final String CMD = "help";
   @Arguments(completer = HelpCompleter.class)
   private final List<String> manPages;

   private final ManFileParser fileParser;
   private final ManProvider manProvider;

   public Help() {
      super();
      this.manProvider = new CliManProvider();
      manPages = new ArrayList<>();
      fileParser = new ManFileParser();
   }

   @Override
   public FileParser getFileParser() {
      return fileParser;
   }

   @Override
   public void displayBottom() throws IOException {
      if (getSearchStatus() == TerminalPage.Search.SEARCHING) {
         clearBottomLine();
         writeToConsole("/" + getSearchWord());
      } else if (getSearchStatus() == TerminalPage.Search.NOT_FOUND) {
         clearBottomLine();
         writeToConsole(ANSI.INVERT_BACKGROUND + "Pattern not found (press RETURN)" + ANSI.DEFAULT_TEXT);
      } else if (getSearchStatus() == TerminalPage.Search.NO_SEARCH ||
            getSearchStatus() == TerminalPage.Search.RESULT) {
         writeToConsole(ANSI.INVERT_BACKGROUND);
         writeToConsole("Manual page " + fileParser.getName() + " line " + getTopVisibleRow() +
               " (press h for help or q to quit)" + ANSI.DEFAULT_TEXT);
      }
   }

   @Override
   public CommandResult execute(CommandInvocation commandInvocation) throws CommandException, InterruptedException {
      if (manPages == null || manPages.size() == 0) {
         commandInvocation.getShell().write("What manual page do you want?" + Config.getLineSeparator());
         return CommandResult.SUCCESS;
      }

      if (manPages.size() <= 0) {
         commandInvocation.getShell().write("No manual entry for " + manPages.get(0) + Config.getLineSeparator());
         return CommandResult.SUCCESS;
      }

      if (manProvider == null) {
         commandInvocation.getShell().write("No manual provider defined");
         return CommandResult.SUCCESS;
      }

      InputStream inputStream = manProvider.getManualDocument(manPages.get(0));
      if (inputStream != null) {
         setCommandInvocation(commandInvocation);
         try {
            fileParser.setInput(inputStream);
            afterAttach();
         } catch (IOException ex) {
            throw new CommandException(ex);
         }
      }

      return CommandResult.SUCCESS;
   }
}
