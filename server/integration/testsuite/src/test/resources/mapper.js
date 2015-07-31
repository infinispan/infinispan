// mode=mapper,reducer=reducer.js,collator=collator.js,language=javascript
var re = /[\W]+/
var unmarshalledValue = marshaller.objectFromByteBuffer(value)
var words = unmarshalledValue.split(re)
for (var i=0; i < words.length; i++) {
   var word = words[i];
   if (word != null) {
      collector.emit(words[i].toLowerCase(), 1);
   }
}