import { XSD } from "@treecg/types";
import { parse } from "csv-parse";
import { createReadStream } from "fs";
import { DataFactory, Quad } from "n3";
import { SW } from "./core";

const { namedNode, literal, quad } = DataFactory;

type Data<T = Quad[]> = { data: T };

function createTimedOutPusher<T extends Quad[]>(things: T[], sw: SW<Data<T>>, onSend?: (item: T) => T) {
    return setTimeout(() => {
        setInterval(() => {
            const thing = things.shift()
            if (thing) {
                const foobar = onSend ? onSend(thing) : thing
                sw.data.push(foobar);
            }
        }, 1000)
    }, 2000);
}

function readCsv(location: string, handler: (item: any[]) => void): Promise<void> {
    return new Promise(res => {
        createReadStream(location)
            .pipe(parse({ delimiter: ",", fromLine: 2 }))
            .on("data", handler)
            .on("close", () => {
                res(undefined);
            })
    });
}

export function readCsvAsRDF(location: string, sw: SW<Data>): Promise<void> {
    let headers: string[] = ["x", "y"];
    const things: Quad[][] = [];

    createTimedOutPusher(things, sw, (things: Quad[]) => [...things, new Quad(things[0].subject, namedNode("http://example.org/ns#time"), literal(new Date().toISOString(), XSD.terms.dateTime))]);

    const handler = (data: any[]) => {
        const out: Quad[] = [];
        const id = namedNode("http://example.org/id/" + Math.random());

        for (let i = 0; i < Math.min(data.length, headers.length); i++) {
            out.push(new Quad(id, namedNode("http://example.org/ns#" + headers[i]), literal(data[i])))
        }

        things.push(out);
    };

    return readCsv(location, handler);
}

export function readCsvFile(location: string, sw: SW<Data>): Promise<void> {
    let headers: string[] = ["x", "y"];
    const things: any[] = [];

    createTimedOutPusher(things, sw);
    const handler = (data: any[]) => {
        const out: any = {};

        for (let i = 0; i < Math.min(data.length, headers.length); i++) {
            out[headers[i]] = data[i];
        }

        things.push(out);
    };

    return readCsv(location, handler);
}