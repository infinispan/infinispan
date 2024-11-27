package org.infinispan.server.resp.commands.json;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.multimap.impl.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter.Indenter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

public class JsonDocGetFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonDocBucket>, String> {
   String space;
   String newline;
   String indent;
   String[] paths;

   static Configuration config = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .options(Option.SUPPRESS_EXCEPTIONS)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider())
         .build();

   public JsonDocGetFunction(String[] paths, String space, String newline, String indent) {
      this.paths = paths;
      this.space = space;
      this.newline = newline;
      this.indent = indent;
   }

   @Override
   public String apply(ReadWriteEntryView<byte[], JsonDocBucket> entryView) {
      Optional<JsonDocBucket> existing = entryView.peek();
      if (existing.isEmpty())
         return null;
      String doc = existing.get().value;
      ObjectMapper mapper = JSONUtil.objectMapper;
      try {
         DefaultPrettyPrinter rpp = (space != null) ? new RespPrettyPrinter(space)
               : new RespPrettyPrinter();
         Indenter ind = new DefaultIndenter(indent, newline);
         rpp.indentArraysWith(ind);
         rpp.indentObjectsWith(ind);

         var rootNode = mapper.readTree(doc);
         var jpCtx = JsonPath.using(config).parse(rootNode);
         // If no path provided return root
         if (paths == null || paths.length == 0) {
            String resp = mapper.writer(rpp).writeValueAsString(rootNode);
            return resp;
         }
         // If only 1 path provided return all the matching nodes as array
         if (paths.length == 1) {
            JsonNode node = jpCtx.read(paths[0]);
            String resp = mapper.writer(rpp).writeValueAsString(node);
            return resp;
         }
         // If more than 1 path provided return an object with
         // properties "path": [array of matching nodes]
         ObjectNode result = mapper.createObjectNode();
         for (String pathStr : paths) {
            JsonNode node = jpCtx.read(pathStr);
            result.set(pathStr, node);
         }
         String resp = mapper.writer(rpp).writeValueAsString(result);
         return resp;
      } catch (JsonProcessingException e) {
         throw new RuntimeException(e);
      }
   }

   private static class Externalizer implements AdvancedExternalizer<JsonDocGetFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonDocGetFunction object) throws IOException {
         output.writeUTF(object.space);
         output.writeUTF(object.newline);
         output.writeUTF(object.indent);
         output.writeInt(object.paths.length);
         for (String path : object.paths) {
            output.writeUTF(path);
         }
      }

      @Override
      public JsonDocGetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String space = input.readUTF();
         String newline = input.readUTF();
         String indent = input.readUTF();
         int length = input.readInt();
         String[] paths = new String[length];
         for (int i = 0; i < length; i++) {
            paths[i] = input.readUTF();
         }
         return new JsonDocGetFunction(paths, space, newline, indent);
      }

      @Override
      public Set<Class<? extends JsonDocGetFunction>> getTypeClasses() {
         return Collections.singleton(JsonDocGetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSONDOC_GET_FUNCTION;
      }
   }
}
