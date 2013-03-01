// mode=mapper,reducer=wordCountReducer.js,collator=wordCountCollator.js,language=javascript
var re = /[\W]+/
var words = value.split(re)
for (var i=0; i < words.length; i++) {
   var word = words[i];
   if (word != null && word.length > 5) {
      collector.emit(words[i].toLowerCase(), 1);
   }
}
