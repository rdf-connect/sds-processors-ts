import { Stream, Writer } from "@ajuvercr/js-runner";

export function streamJoin(inputs: Stream<string>[], output: Writer<string>) {
    let count = 0;

    inputs.forEach(input => {
        input.data(async data => await output.push(data));

        input.on("end", async () => {
            if(count < inputs.length - 1) {
                count += 1;
            } else {
                await output.end();
            }
        });

    });
}