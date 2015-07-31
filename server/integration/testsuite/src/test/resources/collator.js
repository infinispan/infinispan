// mode=collator,language=javascript
var entrySet = reducedResults.entrySet();
var l = new java.util.ArrayList(entrySet);

var results = new java.util.LinkedHashMap();
for (var i = 0; i < entrySet.size(); i++) {
   var entry = l.get(i);
   results.put(entry.getKey(), entry.getValue());
}

results