const Y2R = require('@rmlio/yarrrml-parser/lib/rml-generator.js');
const N3 = require('n3');

function yarrml(reader, writer) {
  console.log(reader, writer);
  const handle = (x) => {
    const y2r = new Y2R();
    const triples = y2r.convert(x);

    const str = new N3.Writer().quadsToString(triples);
    console.log(str);
    return writer.push(str);
  }
  reader.data(handle);
  if(reader.lastElement) {
    handle(reader.lastElement);
  }
}

module.exports.yarrml = yarrml;
