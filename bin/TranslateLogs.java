///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 25

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class TranslateLogs {

   // Map of Suffix -> Language Name for the LLM context
   private static final Map<String, String> LANGUAGES = Map.of("_es", "Spanish", "_fr", "French", "_it", "Italian", "_de", "German", "_pt", "Portuguese");

   private static final String OLLAMA_URL = "http://localhost:11434/api/generate";
   private static final String MODEL = "qwen2.5-coder:7b";//"qwen2.5-coder:14b-instruct-q8_0";

   public static void main(String[] args) throws Exception {
      var projectRoot = Path.of(args.length > 0 ? args[0] : ".");

      System.out.println("🌍 Starting Multi-Language Translation in: " + projectRoot.toAbsolutePath());

      try (var stream = Files.walk(projectRoot)) {
         stream.filter(p -> p.toString().contains("target/generated-translation-files") && p.toString().endsWith(".i18n.properties")).forEach(path -> {
            for (var entry : LANGUAGES.entrySet()) {
               try {
                  processFile(path, entry.getKey(), entry.getValue());
               } catch (Exception e) {
                  throw new RuntimeException(e);
               }
            }
         });
      }

      System.out.println("✅ All modules updated for all languages.");
   }

   private static void processFile(Path sourceFile, String suffix, String langName) throws IOException, InterruptedException {
      var pathStr = sourceFile.toString();
      var parts = pathStr.split("target/generated-translation-files/");
      if (parts.length < 2) return;

      Path moduleRoot = Path.of(parts[0]);
      String relativePkgPath = parts[1];
      String newFilename = relativePkgPath.replace(".properties", suffix + ".properties");
      Path targetFile = moduleRoot.resolve("src/main/resources").resolve(newFilename);

      Properties sourceProps = new Properties();
      try (var stream = Files.newInputStream(sourceFile)) {
         sourceProps.load(stream);
      }
      Properties targetProps = new Properties();
      if (Files.exists(targetFile)) {
         try (var stream = Files.newInputStream(targetFile)) {
            targetProps.load(stream);
         }
      }

      var missingKeys = sourceProps.entrySet().stream().filter(e -> !targetProps.containsKey(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      if (missingKeys.isEmpty()) return;

      System.out.printf("📝 [%s] Translating %d keys for %s%n", langName, missingKeys.size(), targetFile);


      int batchSize = 50;
      List<String> batch = new ArrayList<>(batchSize);

      for (Map.Entry<Object, Object> entry : missingKeys.entrySet()) {
         batch.add(entry.getKey() + "=" + entry.getValue());
         if (batch.size() == batchSize) {
            processBatch(batch, langName, targetProps);
            batch = new ArrayList<>(batchSize);
         }
      }
      if (!batch.isEmpty()) {
         processBatch(batch, langName, targetProps);
      }

      Files.createDirectories(targetFile.getParent());
      try  (var stream = Files.newOutputStream(targetFile)) {
         targetProps.store(stream, "");
      }
   }

   private static void processBatch(List<String> batch, String langName, Properties targetProps) throws IOException, InterruptedException {
      Properties translatedProps = new Properties();
      String payload = String.join("\n", batch);
      String translated = callOllama(payload, langName);
      translatedProps.load(new StringReader(translated));
      targetProps.putAll(translatedProps);
   }

   private static String callOllama(String text, String language) throws IOException, InterruptedException {
      try (var client = HttpClient.newHttpClient()) {
         var body = """
               {
                   "model": "%s",
                   "system": "Translate Java property values to %s. Preserve keys and {0} placeholders. Output ONLY raw properties.",
                   "prompt": %s,
                   "stream": false
               }
               """.formatted(MODEL, language, JSONObject.quote(text));

         var request = HttpRequest.newBuilder().uri(URI.create(OLLAMA_URL)).POST(HttpRequest.BodyPublishers.ofString(body)).build();

         var response = client.send(request, HttpResponse.BodyHandlers.ofString());
         // Clean the response from JSON escaping
         return response.body().split("\"response\":\"")[1].split("\",\"done\"")[0].replace("\\n", "\n").replace("\\\"", "\"");
      }
   }

   private static class JSONObject {
      public static String quote(String s) {
         return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
      }
   }
}
