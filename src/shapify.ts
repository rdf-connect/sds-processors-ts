import { Stream, Writer } from "@rdfc/js-runner";
import { Quad, Term } from "@rdfjs/types";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { Parser, Writer as NWriter } from "n3";
import { RdfStore } from "rdf-stores";
import { Extractor } from "./utils";

export type Shape = {
    id: Term;
    quads: Quad[];
};

export async function shapify(
    reader: Stream<string>,
    writer: Writer<string>,
    shape: Shape,
) {
    const shapeStore = RdfStore.createDefault();
    shape.quads.forEach((q) => shapeStore.addQuad(q));
    const cbd_extractor = new CBDShapeExtractor(shapeStore);
    const extractor = new Extractor(new CBDShapeExtractor(), undefined);

    reader.data(async (x) => {
        const quads = new Parser().parse(x);

        const records = await extractor.parse_records(quads);

        for (const record of records) {
            const store = RdfStore.createDefault();
            record.data.quads.forEach((q) => store.addQuad(q));
            const newData = await cbd_extractor.extract(
                store,
                record.data.id,
                shape.id,
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
            await writer.push(out_str);
        }
    });
}
