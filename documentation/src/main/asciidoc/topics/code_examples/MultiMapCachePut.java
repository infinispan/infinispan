MultimapCache<String, String> multimapCache = ...;

multimapCache.put("girlNames", "marie")
             .thenCompose(r1 -> multimapCache.put("girlNames", "oihana"))
             .thenCompose(r3 -> multimapCache.get("girlNames"))
             .thenAccept(names -> {
                          if(names.contains("marie"))
                              System.out.println("Marie is a girl name");

                           if(names.contains("oihana"))
                              System.out.println("Oihana is a girl name");
                        });
