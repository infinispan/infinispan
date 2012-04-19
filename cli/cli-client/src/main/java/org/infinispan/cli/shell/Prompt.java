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

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author <a href="mailto:lincolnbaxter@gmail.com">Lincoln Baxter, III</a>
 * @author Mike Brock
 * @author Tristan Tarrant
 */
public class Prompt {

   public static String promptExpressionParser(Shell shell, String input) {
      StringBuilder builder = new StringBuilder();
      char[] expr = input.toCharArray();
      Color c = null;

      int i = 0;
      int start = 0;
      for (; i < expr.length; i++) {
         switch (expr[i]) {
         case '\\':
            if (i + 1 < expr.length) {
               /**
                * Handle escape codes here.
                */
               switch (expr[++i]) {
               case '\\':
                  builder.append(new String(expr, start, i - start - 1));
                  builder.append("\\");
                  start = i + 1;
                  break;

               case 'w':
                  builder.append(new String(expr, start, i - start - 1));
                  builder.append(shell.getCWD());
                  start = i + 1;
                  break;

               case 'W':
                  builder.append(new String(expr, start, i - start - 1));
                  String v = shell.getCWD();
                  builder.append(v.substring(v.lastIndexOf('/') + 1));
                  start = i + 1;
                  break;

               case 'd':
                  builder.append(new String(expr, start, i - start - 1));
                  builder.append(new SimpleDateFormat("EEE MMM dd").format(new Date()));
                  start = i + 1;
                  break;

               case 't':
                  builder.append(new String(expr, start, i - start - 1));
                  builder.append(new SimpleDateFormat("HH:mm:ss").format(new Date()));
                  start = i + 1;
                  break;

               case 'T':
                  builder.append(new String(expr, start, i - start - 1));
                  builder.append(new SimpleDateFormat("hh:mm:ss").format(new Date()));
                  start = i + 1;
                  break;

               case '@':
                  builder.append(new String(expr, start, i - start - 1));
                  builder.append(new SimpleDateFormat("KK:mmaa").format(new Date()));
                  start = i + 1;
                  break;

               case '$':
                  builder.append(new String(expr, start, i - start - 1));
                  builder.append("\\$");
                  start = i + 1;
                  break;

               case 'r':
                  builder.append(new String(expr, start, i - start - 1));
                  builder.append("\r");
                  start = i + 1;
                  break;
               case 'n':
                  builder.append(new String(expr, start, i - start - 1));
                  builder.append("\n");
                  start = i + 1;
                  break;

               case 'c':
                  if (i + 1 < expr.length) {
                     switch (expr[++i]) {
                     case '{':
                        boolean nextNodeColor = false;

                        builder.append(new String(expr, start, i - start - 2));

                        start = i;
                        while (i < input.length() && input.charAt(i) != '}')
                           i++;

                        if (i == input.length() && input.charAt(i) != '}') {
                           builder.append(new String(expr, start, i - start));
                        } else {
                           String color = new String(expr, start + 1, i - start - 1);

                           start = ++i;

                           Capture: while (i < expr.length) {
                              switch (expr[i]) {
                              case '\\':
                                 if (i + 1 < expr.length) {
                                    if (expr[i + 1] == 'c') {
                                       if ((i + 2 < expr.length) && expr[i + 2] == '{') {
                                          nextNodeColor = true;
                                       }
                                       break Capture;
                                    }
                                 }

                              default:
                                 i++;
                              }
                           }

                           if (c != null && c != Color.NONE) {
                              builder.append(shell.renderColor(Color.NONE, ""));
                           }

                           c = Color.NONE;
                           for (Color sc : Color.values()) {
                              if (sc.name().equalsIgnoreCase(color == null ? "" : color.trim())) {
                                 c = sc;
                                 break;
                              }
                           }

                           String toColorize = promptExpressionParser(shell, new String(expr, start, i - start));
                           String cStr = shell.renderColor(c, toColorize);

                           builder.append(cStr);
                           if (nextNodeColor) {
                              start = i--;
                           } else {
                              start = i += 2;
                           }
                        }

                        break;

                     default:
                        start = i += 2;
                     }
                  }
               }
            }
         }
      }

      if (start < expr.length && i > start) {
         builder.append(new String(expr, start, i - start));
      }

      return builder.toString();
   }

   public static String echo(Shell shell, String input) {
      char[] expr = input.toCharArray();
      StringBuilder out = new StringBuilder();
      int start = 0;
      int i = 0;
      while (i < expr.length) {
         if (i >= expr.length) {
            break;
         }

         switch (expr[i]) {
         case '\\':
            if (i + 1 < expr.length && expr[i + 1] == '$') {
               out.append(new String(expr, start, i - start));
               out.append('$');
               start = i += 2;
            }
            break;

         case '$':
            out.append(new String(expr, start, i - start));
            start = ++i;
            while (i != expr.length && Character.isJavaIdentifierPart(expr[i]) && expr[i] != 27) {
               i++;
            }

            String var = new String(expr, start, i - start);
            String val = shell.getContext().getProperty(var);
            if (val != null) {
               out.append(String.valueOf(val));
            }

            start = i;
            break;

         default:
            if (Character.isWhitespace(expr[i])) {
               out.append(new String(expr, start, i - start));

               start = i;
               while (i != expr.length && Character.isWhitespace(expr[i])) {
                  i++;
               }

               out.append(new String(expr, start, i - start));

               start = i;
               continue;
            }
         }
         i++;
      }

      if (start < expr.length && i > start) {
         out.append(new String(expr, start, i - start));
      }

      return out.toString();
   }
}
