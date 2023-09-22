import type { Stream, Writer } from "@ajuvercr/js-runner";


export function substitute(reader: Stream<string>, writer: Writer<string>, source: string, replace: string, regexp = false) {
  const reg = regexp ? new RegExp(source) : source;
  reader.data(x => writer.push(x.replace(reg, replace)));
}

export function envsub(reader: Stream<string>, writer: Writer<string> ) {
  var env = process.env;

  reader.data(x => {
    Object.keys(env).forEach(key => {
      const v = env[key];
      if(v) {
        x = x.replace(`\${${key}}`, v)
      }
    });

    return writer.push(x);
  });
}

