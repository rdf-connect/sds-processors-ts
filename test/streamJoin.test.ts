import { describe, test, expect } from "vitest";
import { StreamJoin } from "../lib/streamJoin";
import { createRunner, channel } from "@rdfc/js-runner/lib/testUtils";
import { createLogger, transports } from "winston";
import { strs } from "./utils";
import { FullProc } from "@rdfc/js-runner";

const logger = createLogger({
    transports: [new transports.Console()],
});

describe("Functional tests for streamJoin function", () => {
    test("All data is passed and output is closed properly", async () => {
        const runner = createRunner();
        const [w1, i1] = channel(runner, "w1");
        const [w2, i2] = channel(runner, "w2");
        const [w3, i3] = channel(runner, "w3");
        const [w4, out] = channel(runner, "out");

        const prom = strs(out);

        const proc = <FullProc<StreamJoin>>new StreamJoin(
            {
                output: w4,
                inputs: [i1, i2, i3],
            },
            logger,
        );
        await proc.init();
        proc.transform();

        await Promise.all([
            w1.string("one"),
            w2.string("two"),
            w3.string("three"),
        ]);
        await Promise.all([w1.close(), w2.close(), w3.close()]);
        const strings = await prom;

        expect(strings.includes("one"));
        expect(strings.includes("two"));
        expect(strings.includes("three"));
    });
});
