package org.infinispan.scripting.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.scripting.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ScriptMetadataParser {
   private static final Log log = LogFactory.getLog(ScriptMetadataParser.class, Log.class);
   private static final String DEFAULT_SCRIPT_EXTENSION = "js";
   private static final Pattern METADATA_COMMENT = Pattern.compile("^(?:#|//|;;)\\s*(.+)");
   private static final Pattern METADATA_KV = Pattern
         .compile("\\s*(\\w+)\\s*=\\s*(\"[^\"]*\"|'[^']*'|\\[[\\w,\\s]*]|\\{[\\w,\\s:]*}|[^,=\\s\"]+)\\s*,?");

   public static ScriptMetadata.Builder parse(String name, String script) {
      ScriptMetadata.Builder metadataBuilder = new ScriptMetadata.Builder();

      metadataBuilder.name(name);
      metadataBuilder.mode(ExecutionMode.LOCAL);
      int s = name.lastIndexOf(".") + 1;
      if (s == 0 || s == name.length())
         metadataBuilder.extension(DEFAULT_SCRIPT_EXTENSION);
      else
         metadataBuilder.extension(name.substring(s));

      try (BufferedReader r = new BufferedReader(new StringReader(script))) {
         for (String line = r.readLine(); line != null; line = r.readLine()) {
            Matcher matcher = METADATA_COMMENT.matcher(line);
            if (!matcher.matches())
               break;
            String text = matcher.group(1);
            matcher = METADATA_KV.matcher(text);
            while (matcher.find()) {
               String key = matcher.group(1).toLowerCase();
               String value = Util.unquote(matcher.group(2));
               switch (key) {
               case "name":
                  metadataBuilder.name(value);
                  break;
               case "extension":
                  metadataBuilder.extension(value);
                  break;
               case "language":
                  metadataBuilder.language(value);
                  break;
               case "mode":
                  metadataBuilder.mode(ExecutionMode.valueOf(value.toUpperCase()));
                  break;
               case "parameters":
                  metadataBuilder.parameters(unarray(value));
                  break;
               case "role":
                  metadataBuilder.role(value);
                  break;
               case "datatype":
                  metadataBuilder.dataType(MediaType.fromString(value));
                  break;
               case "properties":
                  metadataBuilder.properties(unobject(value));
                  break;
               default:
                  throw log.unknownScriptProperty(key);
               }
            }
         }
      } catch (IOException e) {
         // Ignore
      }

      return metadataBuilder;
   }

   private static Set<String> unarray(String s) {
      if (s.charAt(0) == '[') {
         String[] ps = s.substring(1, s.length() - 1).split("\\s*,\\s*");
         Set<String> parameters = new HashSet<>(ps.length);
         Collections.addAll(parameters, ps);
         return parameters;
      } else {
         throw log.parametersNotArray();
      }
   }

   private static Map<String, String> unobject(String s) {
      if (s.charAt(0) == '{') {
         String[] ps = s.substring(1, s.length() - 1).split("\\s*,\\s*");
         Map<String, String> properties = new HashMap<>(ps.length);
         for(String p : ps) {
            String[] kv = p.split(":");
            properties.put(kv[0].trim(), kv[1].trim());
         }
         return properties;
      } else {
         throw log.propertiesNotObject();
      }
   }
}
