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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;

import org.infinispan.cli.Config;

/**
 * ConfigImpl.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ConfigImpl implements Config {
   private static final String CONFIG_FILE = "ispn-cli.ini";
   private final String configHome;
   private boolean colorsEnabled = true;
   private boolean historyEnabled = true;
   private String prompt = "[\\c{green}$CONNECTION\\c{yellow}/$CONTAINER\\c{yellow}/$CACHE\\c]> ";

   public ConfigImpl(String configHome) {
      this.configHome = configHome;
   }

   @Override
   public void load() {
      Properties p = new Properties();
      File configFile = new File(configHome, CONFIG_FILE);
      if (configFile.exists()) {
         try {
            Reader r = new BufferedReader(new FileReader(configFile));
            p.load(r);
            r.close();
            if (p.containsKey("colors")) colorsEnabled = Boolean.parseBoolean(p.getProperty("colors"));
            if (p.containsKey("history")) historyEnabled = Boolean.parseBoolean(p.getProperty("history"));
            if (p.containsKey("prompt")) prompt = p.getProperty("prompt");
         } catch (IOException e) {
            //FIXME implement me
         }
      }
   }

   @Override
   public void save() {
      Properties p = new Properties();
      p.setProperty("colors", String.valueOf(colorsEnabled));
      p.setProperty("history", String.valueOf(historyEnabled));
      p.setProperty("prompt", prompt);
      File configFile = new File(configHome, CONFIG_FILE);
      try {
         Writer w = new BufferedWriter(new FileWriter(configFile));
         p.store(w, null);
         w.close();
      } catch (IOException e) {
         //FIXME implement me
      }
   }

   @Override
   public boolean isColorEnabled() {
      return colorsEnabled;
   }

   @Override
   public boolean isHistoryEnabled() {
      return historyEnabled;
   }

   @Override
   public String getPrompt() {
      return prompt;
   }
}
