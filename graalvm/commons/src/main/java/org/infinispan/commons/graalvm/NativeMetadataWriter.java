package org.infinispan.commons.graalvm;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public class NativeMetadataWriter {
   public static void main(String[] args) throws Exception {
      if (args.length == 0)
         throw new IllegalArgumentException(String.format("FQN of '%s' implementation must be provided as the first arg", NativeMetadataProvider.class));

      NativeMetadataProvider metadata = ((Class<NativeMetadataProvider>) Class.forName(args[0])).getConstructor().newInstance();
      String rootDir = args.length > 1 ? args[1] : "";
      Files.createDirectories(Paths.get(rootDir));

      Json reflection = array(metadata.reflectiveClasses());

      Json resource = Json.object()
            .set("resources",
                  Json.object().set("includes", array(metadata.includedResources()))
            )
            .set("bundles",
                  array(metadata.bundles())
            );

      Files.writeString(Paths.get(rootDir, "reflection-config.json"), reflection.toPrettyString());
      Files.writeString(Paths.get(rootDir, "resource-config.json"), resource.toPrettyString());
   }

   private static Json array(Stream<? extends JsonSerialization> stream) {
      return Json.make(
            stream
                  .map(JsonSerialization::toJson)
                  .collect(Collectors.toList())
      );
   }
}
