import { Processor, type Reader, type Writer } from "@rdfc/js-runner";

type Args = {
    inputs: Reader[];
    output: Writer;
};
export class StreamJoin extends Processor<Args> {
    async init(this: Args & this): Promise<void> {
        // nothing
    }
    async transform(this: Args & this): Promise<void> {
        // TODO this should be better
        await Promise.all(this.inputs.map((x) => this.setupReader(x)));
        await this.output.close();
    }
    async produce(this: Args & this): Promise<void> {
        // nothing
    }
    async setupReader(this: Args & this, reader: Reader) {
        for await (const x of reader.strings()) {
            await this.output.string(x);
        }
    }
}
