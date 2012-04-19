/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.shell;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.File;

import org.fusesource.jansi.Ansi;
import org.infinispan.cli.Config;
import org.infinispan.cli.Context;
import org.infinispan.cli.commands.Command;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.commands.client.Version;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.ConnectionFactory;
import org.infinispan.cli.impl.CommandBufferImpl;
import org.infinispan.cli.impl.ContextImpl;
import org.infinispan.cli.io.ConsoleIOAdapter;
import org.infinispan.cli.io.StreamIOAdapter;
import org.infinispan.cli.util.SystemUtils;
import org.jboss.jreadline.console.Console;
import org.jboss.jreadline.console.settings.Settings;

/**
 *
 * ShellImpl.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ShellImpl implements Shell {
   private Config config;
   private Console console;
   private Context context;

   @Override
   public void init(String[] args) throws Exception {
      // Initialize the context for simple standard I/O
      context = new ContextImpl(new StreamIOAdapter(), new CommandBufferImpl());
      String sopts = "hc:v";
      LongOpt[] lopts = new LongOpt[] {
            new LongOpt("connect", LongOpt.REQUIRED_ARGUMENT, null, 'c'),
            new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h'),
            new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'v'),
      };
      Getopt g = new Getopt("ispn-cli", args, sopts, lopts);
      int c;
      while ((c = g.getopt()) != -1) {
         switch (c) {
         case 'c': {
            Connection connection = ConnectionFactory.getConnection(g.getOptarg());
            connection.connect(context);
            context.setConnection(connection);
            break;
         }
         case 'h':
            help();
            break;
         case 'v':
            version();
            break;
         }
      }
      config = new ConfigImpl(SystemUtils.getAppConfigFolder("InfinispanShell"));
      config.load();
      Settings settings = Settings.getInstance();
      settings.setReadInputrc(false);

      // FIXME: only if we're going interactive
      console = new Console(settings);
      context.setOutputAdapter(new ConsoleIOAdapter(console));
      console.addCompletion(new Completer(context));
   }

   @Override
   public void run() {
      while (!context.isQuitting()) {
         try {
            context.refreshProperties();
            String line = console.read(getPrompt());

            if (line != null) {
               if (!"".equals(line.trim())) {
                  execute(line);
               }
            }
         } catch (Exception e) {
            context.error(e);
         }
      }
      try {
         config.save();
         console.stop();
      } catch (Exception e) {
      }
   }

   private void execute(String line) {
      ProcessedCommand parsed = new ProcessedCommand(line);
      Command command = context.getCommandRegistry().getCommand(parsed.getCommand());
      if (command != null) {
         command.execute(context, parsed);
      } else {
         context.println("Command " + parsed.getCommand() + " unknown or not available");
      }
   }

   private String getPrompt() {
      return Prompt.echo(this, Prompt.promptExpressionParser(this, config.getPrompt()));
   }

   @Override
   public String renderColor(final Color color, final String output) {
      if (!config.isColorEnabled()) {
         return output;
      }

      Ansi ansi = new Ansi();

      switch (color) {
      case BLACK:
         ansi.fg(Ansi.Color.BLACK);
         break;
      case BLUE:
         ansi.fg(Ansi.Color.BLUE);
         break;
      case CYAN:
         ansi.fg(Ansi.Color.CYAN);
         break;
      case GREEN:
         ansi.fg(Ansi.Color.GREEN);
         break;
      case MAGENTA:
         ansi.fg(Ansi.Color.MAGENTA);
         break;
      case RED:
         ansi.fg(Ansi.Color.RED);
         break;
      case WHITE:
         ansi.fg(Ansi.Color.WHITE);
         break;
      case YELLOW:
         ansi.fg(Ansi.Color.YELLOW);
         break;
      case BOLD:
         ansi.a(Ansi.Attribute.INTENSITY_BOLD);
         break;
      case ITALIC:
         ansi.a(Ansi.Attribute.ITALIC);
         ansi.a(Ansi.Attribute.INTENSITY_FAINT);
         break;

      default:
         return output;
      }

      return ansi.render(output).reset().toString();
   }

   @Override
   public String getCWD() {
      File directory = new File(".");
      return directory.getAbsolutePath();
   }

   @Override
   public Context getContext() {
      return context;
   }

   private void help() {
      System.out.println("Usage: ispn-cli [OPTION]...");
      System.out.println("Command-line interface for interacting with a running instance of Infinispan");
      System.out.println();
      System.out.println("Options:");
      System.out.println("  -c, --connect=URL       connects to a running instance of Infinispan. Currently only ");
      System.out.println("                          supports JMX via the following URL format: jmx://host:port");
      System.out.println("  -h, --help              shows this help page");
      System.out.println("  -v, --version           shows version information");
      System.exit(0);
   }

   private void version() {
      System.out.println("ispn-cli " + Version.class.getPackage().getImplementationVersion());
      System.out.println("Copyright (C) 2012 Red Hat Inc. and/or its affiliates and other contributors");
      System.out.println("License GNU Lesser General Public License, v. 2.1. http://www.gnu.org/licenses/lgpl-2.1.txt");
      System.exit(0);
   }
}
