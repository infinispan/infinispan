// mode=collator,language=javascript
var entrySet = reducedResults.entrySet();
var l = new java.util.ArrayList(entrySet);
java.util.Collections.sort(l, new org.infinispan.scripting.EntryComparator())

var results = new java.util.LinkedHashMap();
for (var i=0; i<20; i++) {
   var entry = l.get(i);
   results.put(entry.getKey(), entry.getValue());
}

results
