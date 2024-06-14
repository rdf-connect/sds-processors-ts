import { describe, test, expect } from "vitest";
import { SimpleStream } from "@rdfc/js-runner";
import { streamJoin } from "../src/streamJoin";

describe("Functional tests for streamJoin function", () => {
    test("All data is passed and output is closed properly", async () => {
        const i1 = new SimpleStream<string>();
        const i2 = new SimpleStream<string>();
        const i3 = new SimpleStream<string>();
        const out = new SimpleStream<string>();
        
        let dataRecord = "";
        out.data(data => {
            dataRecord += data;
        }).on("end", () => {
            expect(dataRecord.includes("one"));
            expect(dataRecord.includes("two"));
            expect(dataRecord.includes("three"));
        });

        await streamJoin([i1, i2, i3], out);

        await Promise.all([
            i1.push("one"),
            i2.push("two"),
            i3.push("three")
        ]);

        await Promise.all([
            i1.end(),
            i2.end(),
            i3.end()
        ]);
    });
});