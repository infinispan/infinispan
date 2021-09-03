Map<Object, String> jbossValues =
cache.entrySet().stream()
     .filter(e -> e.getValue().contains("JBoss"))
     .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
