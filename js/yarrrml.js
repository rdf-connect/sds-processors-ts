import Y2R from '@rmlio/yarrrml-parser/lib/rml-generator.js';
import { Writer } from 'n3';

function yarrml(reader, writer) {

  const handle = (x) => {
    const y2r = new Y2R();
    const triples = y2r.convert(x);

    const str = new Writer().quadsToString(triples);
    return writer.push(str);
  }

  reader.data(handle);
  if(reader.lastElement) {
    handle(reader.lastElement);
  }

}

const _yarrml = yarrml;
export { _yarrml as yarrml };
