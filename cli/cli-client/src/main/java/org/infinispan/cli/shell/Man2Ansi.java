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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.Ansi.Attribute;

/**
 * A very simple and incomplete converter from troff-style man macro syntax to ansi
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class Man2Ansi {
   public static Pattern MAN_MACRO_REGEX = Pattern.compile("^(\\.[A-Z]{1,2} ?)?(.*)$");
   public static int DEFAULT_INDENT = 4;
   public static int WRAP_WIDTH = 72;
   private final Ansi ansi = new Ansi();
   private int pos = 0;
   private int indent = DEFAULT_INDENT;
   private boolean blankLine = true;
   private int screenWidth;


   public Man2Ansi(int screenWidth) {
      this.screenWidth = screenWidth;
   }

   public String render(InputStream is) throws IOException {
      BufferedReader r = new BufferedReader(new InputStreamReader(is));
      for (String line = r.readLine(); line != null; line = r.readLine()) {
         Matcher matcher = MAN_MACRO_REGEX.matcher(line);
         if (matcher.matches()) {
            String macro = matcher.group(1);
            String text = matcher.group(2);
            if (".B ".equals(macro)) {
               fit(text, Attribute.INTENSITY_BOLD);
            } else if (".I ".equals(macro)) {
               fit(text, Attribute.ITALIC);
            } else if (".SH ".equals(macro)) {
               newline(false);
               flushLeft();
               newline(true);
               fit(text, Attribute.INTENSITY_BOLD);
               resetIndent();
               newline(true);
            } else if (".IP ".equals(macro)) {
               resetIndent();
               newline(false);
               fit(text);
               tab(DEFAULT_INDENT);
            } else if (".BR".equals(macro)) {
               newline(false);
            } else {
               fit(text);
            }
         }
      }
      return ansi.toString();
   }

   private void newline(boolean force) {
      if(force || !blankLine) {
         ansi.newline();
         indent();
      }
   }

   private void fit(String text, Attribute... attributes) {
      if (pos + text.length() > screenWidth) {
         int ideal = screenWidth-pos;
         int actual = text.lastIndexOf(' ', ideal);
         if (actual>0) {
            format(text.substring(0, actual));
            ansi.newline();
            indent();
            fit(text.substring(actual+1), attributes);
            return;
         } else {
            ansi.newline();
            indent();
         }
      }
      format(text, attributes);
      if (text.charAt(text.length()-1) != ' ')
         format(" ", attributes);
      pos += text.length();
      blankLine = false;
   }

   private void format(String text, Attribute... attributes) {
      for (Attribute attribute : attributes) {
         ansi.a(attribute);
      }
      ansi.render(text).reset();
   }

   private void tab(int add) {
      indent += add;
      if (pos>=indent) {
         newline(false);
      } else {
         for(; pos<indent; pos++) {
            ansi.render(" ");
         }
      }
   }

   private void indent() {
      indent(0);
   }

   private void indent(int add) {
      indent += add;
      for (int i = 0; i < indent; i++) {
         ansi.render(" ");
      }
      pos = indent;
      blankLine = true;
   }

   private void resetIndent() {
      indent = DEFAULT_INDENT;
   }

   private void flushLeft() {
      indent = 0;
   }
}
