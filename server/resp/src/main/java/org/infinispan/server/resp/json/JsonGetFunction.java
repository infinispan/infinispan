package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.server.resp.ExternalizerIds;
import org.infinispan.util.function.SerializableFunction;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter.Indenter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonGetFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], JsonBucket>, byte[]> {
   public static final AdvancedExternalizer<JsonGetFunction> EXTERNALIZER = new JsonGetFunction.Externalizer();
   static final byte[] EMPTY_BYTES = new byte[0];
   public static final String ERR_PATHS_CAN_T_BE_NULL = "paths can't be null";

   byte[] space;
   byte[] newline;
   byte[] indent;
   List<byte[]> paths;
   private boolean isLegacy;

   public JsonGetFunction(List<byte[]> paths, byte[] space, byte[] newline, byte[] indent) {
      requireNonNull(paths, ERR_PATHS_CAN_T_BE_NULL);
      this.paths = paths;
      this.space = (space != null) ? space : EMPTY_BYTES;
      this.newline = (newline != null) ? newline : EMPTY_BYTES;
      this.indent = (indent != null) ? indent : EMPTY_BYTES;
      isLegacy = true;
   }

   @Override
   public byte[] apply(ReadWriteEntryView<byte[], JsonBucket> entryView) {
      Optional<JsonBucket> existing = entryView.peek();
      if (existing.isEmpty())
         return null;
      byte[] doc = existing.get().value();
      ObjectMapper mapper = JSONUtil.objectMapper;
      try {
         DefaultPrettyPrinter rpp = (space.length > 0)
               ? new RespPrettyPrinter(new String(space, StandardCharsets.UTF_8))
               : new RespPrettyPrinter();
         Indenter ind = new DefaultIndenter(new String(indent, StandardCharsets.UTF_8),
               new String(newline, StandardCharsets.UTF_8));
         rpp.indentArraysWith(ind);
         rpp.indentObjectsWith(ind);

         var rootNode = mapper.readTree(new String(doc, StandardCharsets.UTF_8));
         var jpCtx = JSONUtil.parserForGet.parse(rootNode);
         // If no path provided return root in legacy format
         if (paths == null || paths.size() == 0) {
            byte[] resp = mapper.writer(rpp).writeValueAsBytes(rootNode);
            return resp;
         }
         // Convert to jsonpath and set legacy=false if any path is a jsonpath
         List<byte[]> jsonPaths = paths.stream().map((p) -> {
            var jp = JSONUtil.toJsonPath(p);
            isLegacy &= (jp != p);
            return jp;
         }).toList();

         // If only 1 path provided, return all the matching nodes as array
         if (jsonPaths.size() == 1) {
            String pathStr = new String(jsonPaths.get(0), StandardCharsets.UTF_8);
            ArrayNode nodeList = jpCtx.read(pathStr);
            // If legacy return just the first one
            if (isLegacy) {
               if (nodeList.size()==0) {
                  throw new RuntimeException("Path '"+pathStr+"' does not exist");
               }
               byte[] resp = mapper.writer(rpp).writeValueAsBytes(nodeList.get(0));
               return resp;
            }
            byte[] resp = mapper.writer(rpp).writeValueAsBytes(nodeList);
            return resp;
         }
         // If more than 1 path provided return an object with
         // properties "path": [array of matching nodes]

         ObjectNode result = mapper.createObjectNode();
         for (int i = 0; i < jsonPaths.size(); i++) {
            var jsonPath = jsonPaths.get(i);
            var path = paths.get(i);
            var jsonPathStr = new String(jsonPath, StandardCharsets.UTF_8);
            // Result should contain original paths as keys
            var pathStr = new String(path, StandardCharsets.UTF_8);
            ArrayNode nodeList = (ArrayNode)jpCtx.read(jsonPathStr);
            // If legacy return just the first one
            if (isLegacy) {
               if (nodeList.size()==0) {
                  throw new RuntimeException("Path '"+jsonPathStr+"' does not exist");
               }
               result.set(pathStr, mapper.valueToTree(nodeList.get(0)));
            } else {
               result.set(pathStr, mapper.valueToTree(nodeList));
            }
         }
         byte[] resp = mapper.writer(rpp).writeValueAsBytes(result);
         return resp;
      } catch (JsonProcessingException e) {
         throw new RuntimeException(e);
      }
   }

   private static class Externalizer implements AdvancedExternalizer<JsonGetFunction> {

      @Override
      public void writeObject(ObjectOutput output, JsonGetFunction object) throws IOException {
         JSONUtil.writeBytes(output, object.space);
         JSONUtil.writeBytes(output, object.newline);
         JSONUtil.writeBytes(output, object.indent);
         output.writeInt(object.paths.size());
         for (byte[] path : object.paths) {
            JSONUtil.writeBytes(output, path);
         }
      }

      @Override
      public JsonGetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] space = JSONUtil.readBytes(input);
         byte[] newline = JSONUtil.readBytes(input);
         byte[] indent = JSONUtil.readBytes(input);
         int length = input.readInt();
         List<byte[]> paths = new ArrayList<>();
         for (int i = 0; i < length; i++) {
            paths.add(JSONUtil.readBytes(input));
         }
         return new JsonGetFunction(paths, space, newline, indent);
      }

      @Override
      public Set<Class<? extends JsonGetFunction>> getTypeClasses() {
         return Collections.singleton(JsonGetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JSON_GET_FUNCTION;
      }
   }
}

class RespPrettyPrinter extends DefaultPrettyPrinter {
   private final String ofvs;

   public RespPrettyPrinter() {

      super();
      ofvs = ":";
   }

   public RespPrettyPrinter(String objectFieldValueSeparator) {
      super();
      super._arrayEmptySeparator = "";
      super._objectEmptySeparator = "";
      ofvs = ":" + objectFieldValueSeparator;
   }

   public RespPrettyPrinter(RespPrettyPrinter base) {
      super(base);
      super._arrayEmptySeparator = "";
      super._objectEmptySeparator = "";
      this.ofvs = base.ofvs;
   }

   @Override
   public void writeObjectFieldValueSeparator(JsonGenerator g) throws IOException {
      g.writeRaw(ofvs);
   }

   @Override
   public DefaultPrettyPrinter createInstance() {
      return new RespPrettyPrinter(this);
   }
}
