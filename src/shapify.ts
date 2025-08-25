import { Processor, type Reader, type Writer } from "@rdfc/js-runner";
import { Quad, Term } from "@rdfjs/types";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { Parser, Writer as NWriter } from "n3";
import { RdfStore } from "rdf-stores";
import { Extractor } from "./utils";

export type Shape = {
    id: Term;
    quads: Quad[];
};
type Args = {
    reader: Reader;
    writer: Writer;
    shape: Shape;
};
export class Shapify extends Processor<Args> {
    cbd_extractor: CBDShapeExtractor;
    extractor: Extractor;
    async init(this: Args & this): Promise<void> {
        const shapeStore = RdfStore.createDefault();
        this.shape.quads.forEach((q) => shapeStore.addQuad(q));
        this.cbd_extractor = new CBDShapeExtractor(shapeStore);
        this.extractor = new Extractor(new CBDShapeExtractor(), undefined);
    }
    async transform(this: Args & this): Promise<void> {
        for await (const x of this.reader.strings()) {
            const quads = new Parser().parse(x);

            const records = await this.extractor.parse_records(quads);

            for (const record of records) {
                const store = RdfStore.createDefault();
                record.data.quads.forEach((q) => store.addQuad(q));
                const newData = await this.cbd_extractor.extract(
                    store,
                    record.data.id,
                    this.shape.id,
                );

                const n_writer = new NWriter({ format: "text/turtle" });

                n_writer.addQuads(
                    quads.filter(
                        (x) =>
                            x.graph.value ===
                            "https://w3id.org/sds#DataDescription",
                    ),
                );
                n_writer.addQuads(newData);

                const out_str: string = await new Promise((res, rej) =>
                    n_writer.end((error, result) => {
                        if (error) rej(error);
                        res(result);
                    }),
                );
                await this.writer.string(out_str);
            }
        }
    }
    async produce(this: Args & this): Promise<void> {
        // nothing
    }
}
