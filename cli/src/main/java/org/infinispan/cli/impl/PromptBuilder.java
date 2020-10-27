package org.infinispan.cli.impl;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.aesh.readline.terminal.formatting.Color;
import org.aesh.terminal.utils.ANSI;
import org.infinispan.cli.Context;

/**
 * @author <a href="mailto:lincolnbaxter@gmail.com">Lincoln Baxter, III</a>
 * @author Mike Brock
 * @author Tristan Tarrant
 */
public class PromptBuilder {

   public static String promptExpressionParser(Context context, String input) {
      StringBuilder builder = new StringBuilder();
      char[] expr = input.toCharArray();
      Color c = null;

      int i = 0;
      int start = 0;
      for (; i < expr.length; i++) {
         switch (expr[i]) {
            case '$':
               builder.append(new String(expr, start, i - start));
               start = ++i;
               while (i != expr.length && Character.isJavaIdentifierPart(expr[i]) && expr[i] != 27) {
                  i++;
               }
               String var = new String(expr, start, i - start);
               String val = context.getProperty(var);
               if (val != null) {
                  builder.append(val);
               }
               start = i;
               break;
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
                        builder.append(context.getCurrentWorkingDirectory().getAbsolutePath());
                        start = i + 1;
                        break;

                     case 'W':
                        builder.append(new String(expr, start, i - start - 1));
                        String v = context.getCurrentWorkingDirectory().getAbsolutePath();
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
                                    Capture:
                                    while (i < expr.length) {
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
                                    for (Color sc : Color.values()) {
                                       if (sc.name().equalsIgnoreCase(color.trim())) {
                                          c = sc;
                                          break;
                                       }
                                    }
                                    switch (c) {
                                       case RED:
                                          builder.append(ANSI.RED_TEXT);
                                          break;
                                       case BLUE:
                                          builder.append(ANSI.BLUE_TEXT);
                                          break;
                                       case CYAN:
                                          builder.append(ANSI.CYAN_TEXT);
                                          break;
                                       case GREEN:
                                          builder.append(ANSI.GREEN_TEXT);
                                          break;
                                       case YELLOW:
                                          builder.append(ANSI.YELLOW_TEXT);
                                          break;
                                       case WHITE:
                                          builder.append(ANSI.WHITE_TEXT);
                                          break;
                                       case MAGENTA:
                                          builder.append(ANSI.MAGENTA_TEXT);
                                          break;
                                       case BLACK:
                                          builder.append(ANSI.BLACK_TEXT);
                                          break;
                                       case DEFAULT:
                                          builder.append(ANSI.DEFAULT_TEXT);
                                          break;
                                    }
                                    builder.append(promptExpressionParser(context, new String(expr, start, i - start)));
                                    builder.append(ANSI.RESET);
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
}
