import type { Reader } from "@rdfc/js-runner";

export async function strs(reader: Reader): Promise<string[]> {
    const out: string[] = [];

    for await (const st of reader.strings()) {
        out.push(st);
    }

    return out;
}
export async function readStrings(reader: Reader, strings: string[]) {
    for await (const st of reader.strings()) {
        strings.push(st);
    }
}
