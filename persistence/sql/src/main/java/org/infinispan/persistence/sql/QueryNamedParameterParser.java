package org.infinispan.persistence.sql;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class QueryNamedParameterParser {
   private static final Log log = LogFactory.getLog(QueryNamedParameterParser.class, Log.class);

   /**
    * Set of characters that qualify as comment or quotes starting characters.
    */
   private static final String[] START_SKIP = new String[]{"'", "\"", "--", "/*"};

   /**
    * Set of characters that at are the corresponding comment or quotes ending characters.
    */
   private static final String[] STOP_SKIP = new String[]{"'", "\"", "\n", "*/"};

   /**
    * Set of characters that qualify as parameter separators,
    * indicating that a parameter name in an SQL String has ended.
    */
   private static final String PARAMETER_SEPARATORS = "\"':&,;()|=+-*%/\\<>^";

   /**
    * An index with separator flags per character code.
    * Technically only needed between 34 and 124 at this point.
    */
   private static final boolean[] separatorIndex = new boolean[128];

   static {
      for (char c : PARAMETER_SEPARATORS.toCharArray()) {
         separatorIndex[c] = true;
      }
   }


   //-------------------------------------------------------------------------
   // Core methods used by NamedParameterJdbcTemplate and SqlQuery/SqlUpdate
   //-------------------------------------------------------------------------

   /**
    * Parse the SQL statement and locate any placeholders or named parameters. Named parameters are substituted for a
    * JDBC placeholder.
    *
    * @param sql the SQL statement
    * @return the parsed statement, represented as ParsedSql instance
    */
   public static ParserResults parseSqlStatement(final String sql) {
      StringBuilder sqlToUse = new StringBuilder(sql);
      List<String> parameterList = new ArrayList<>();

      char[] statement = sql.toCharArray();

      int escapes = 0;
      int i = 0;
      while (i < statement.length) {
         int skipToPosition;
         while (i < statement.length) {
            skipToPosition = skipCommentsAndQuotes(statement, i);
            if (i == skipToPosition) {
               break;
            }
            else {
               i = skipToPosition;
            }
         }
         if (i >= statement.length) {
            break;
         }
         char c = statement[i];
         if (c == ':' || c == '&') {
            int j = i + 1;
            if (c == ':' && j < statement.length && statement[j] == ':') {
               // Postgres-style "::" casting operator should be skipped
               i = i + 2;
               continue;
            }
            String parameter;
            if (c == ':' && j < statement.length && statement[j] == '{') {
               // :{x} style parameter
               while (statement[j] != '}') {
                  j++;
                  if (j >= statement.length) {
                     throw log.nonTerminatedNamedParamInSql(i, sql);
                  }
                  if (statement[j] == ':' || statement[j] == '{') {
                     throw log.invalidCharacterInSql(statement[j], i, sql);
                  }
               }
               if (j - i > 2) {
                  sqlToUse.replace(i - escapes, j - escapes, "?");
                  escapes += j - i - 1;
                  parameter = sql.substring(i + 2, j);
                  parameterList.add(parameter);
               }
               j++;
            }
            else {
               while (j < statement.length && !isParameterSeparator(statement[j])) {
                  j++;
               }
               if (j - i > 1) {
                  sqlToUse.replace(i - escapes, j - escapes, "?");
                  escapes += j - i - 1;
                  parameter = sql.substring(i + 1, j);
                  parameterList.add(parameter);
               }
            }
            i = j - 1;
         }
         else {
            if (c == '\\') {
               int j = i + 1;
               if (j < statement.length && statement[j] == ':') {
                  // escaped ":" should be skipped
                  sqlToUse.deleteCharAt(i - escapes);
                  escapes++;
                  i = i + 2;
                  continue;
               }
            }
            if (c == '?') {
               throw log.unnamedParametersNotAllowed(i, sql);
            }
         }
         i++;
      }
      return new ParserResults(sqlToUse.toString(), parameterList);
   }

   /**
    * Skip over comments and quoted names present in an SQL statement.
    * @param statement character array containing SQL statement
    * @param position current position of statement
    * @return next position to process after any comments or quotes are skipped
    */
   private static int skipCommentsAndQuotes(char[] statement, int position) {
      for (int i = 0; i < START_SKIP.length; i++) {
         if (statement[position] == START_SKIP[i].charAt(0)) {
            boolean match = true;
            for (int j = 1; j < START_SKIP[i].length(); j++) {
               if (statement[position + j] != START_SKIP[i].charAt(j)) {
                  match = false;
                  break;
               }
            }
            if (match) {
               int offset = START_SKIP[i].length();
               for (int m = position + offset; m < statement.length; m++) {
                  if (statement[m] == STOP_SKIP[i].charAt(0)) {
                     boolean endMatch = true;
                     int endPos = m;
                     for (int n = 1; n < STOP_SKIP[i].length(); n++) {
                        if (m + n >= statement.length) {
                           // last comment not closed properly
                           return statement.length;
                        }
                        if (statement[m + n] != STOP_SKIP[i].charAt(n)) {
                           endMatch = false;
                           break;
                        }
                        endPos = m + n;
                     }
                     if (endMatch) {
                        // found character sequence ending comment or quote
                        return endPos + 1;
                     }
                  }
               }
               // character sequence ending comment or quote not found
               return statement.length;
            }
         }
      }
      return position;
   }

   /**
    * Determine whether a parameter name ends at the current position, that is, whether the given character qualifies as
    * a separator.
    */
   private static boolean isParameterSeparator(char c) {
      return (c < 128 && separatorIndex[c]) || Character.isWhitespace(c);
   }

   public static class ParserResults {
      private final String sqlToUse;
      private final List<String> orderedParameters;

      public ParserResults(String sqlToUse, List<String> orderedParameters) {
         this.sqlToUse = sqlToUse;
         this.orderedParameters = orderedParameters;
      }

      public String getSqlToUse() {
         return sqlToUse;
      }

      public List<String> getOrderedParameters() {
         return orderedParameters;
      }
   }
}
