import { Processor, type Reader, type Writer } from "@rdfc/js-runner";
import { RdfStore } from "rdf-stores";
import { Parser, Writer as N3Writer } from "n3";
import { SDS } from "@treecg/types";
import { Quad_Graph } from "@rdfjs/types";

type Args = {
    input: Reader;
    output: Writer;
};

export class MemberAsNamedGraph extends Processor<Args> {
    async init(this: Args & this): Promise<void> {
        // nothing
    }
    async transform(this: Args & this): Promise<void> {
        for await (const data of this.input.strings()) {
            const store = RdfStore.createDefault();
            new Parser().parse(data).forEach((quad) => store.addQuad(quad));

            // Extract the member from the SDS record
            const memberQ = store.getQuads(null, SDS.terms.payload, null)[0];

            if (!memberQ) {
                throw new Error(`No payload found in SDS record: \n${data}`);
            } else {
                const member = memberQ.object;
                const memberStore = RdfStore.createDefault();

                // Add SDS quads to newly created memberStore and remove them from the original store
                memberStore.addQuad(memberQ);
                store.removeQuad(memberQ);
                const streamQ = store.getQuads(null, SDS.terms.stream, null)[0];
                memberStore.addQuad(streamQ);
                store.removeQuad(streamQ);

                // Add all quads of the member to the memberStore and remove them from the original store
                store.getQuads(member).forEach((quad) => {
                    memberStore.addQuad(quad);
                    store.removeQuad(quad);
                });

                // Add all other quads present in the original member in a named graph to the memberStore
                store.getQuads().forEach((quad) => {
                    quad.graph = <Quad_Graph>member;
                    memberStore.addQuad(quad);
                });

                await this.output.string(
                    new N3Writer().quadsToString(memberStore.getQuads()),
                );
            }
        }

        await this.output.close();
    }
    async produce(this: Args & this): Promise<void> {
        // nothing
    }
}
