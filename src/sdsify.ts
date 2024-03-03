import type { Stream, Writer } from "@ajuvercr/js-runner";
import { LDES, RDF, SDS, SHACL, XSD } from "@treecg/types";
import type { Quad, Term } from "@rdfjs/types";
import { blankNode, namedNode } from "./core.js";
import { DataFactory, Parser, Quad_Object, Store, Writer as NWriter, Quad_Subject, NamedNode } from "n3";
import { CBDShapeExtractor } from "extract-cbd-shape";
import { createHash } from "crypto";

function maybe_parse(data: Quad[] | string): Quad[] {
   if (typeof data === "string" || data instanceof String) {
      const parse = new Parser();
      return parse.parse(<string>data);
   } else {
      return data;
   }
}

// Find the main sh:NodeShape subject of a give Shape Graph.
// We determine this by assuming that the main node shape
// is not referenced by any other shape description.
// If more than one is found an exception is thrown.
function extractMainNodeShape(store: Store): Quad_Subject {
   const nodeShapes = store.getSubjects(RDF.type, SHACL.NodeShape, null);
   let mainNodeShape = null;

   if (nodeShapes && nodeShapes.length > 0) {
      for (const ns of nodeShapes) {
         const isNotReferenced = store.getSubjects(null, ns, null).length === 0;

         if (isNotReferenced) {
            if (!mainNodeShape) {
               mainNodeShape = ns;
            } else {
               throw new Error("There are multiple main node shapes in a given shape. Unrelated shapes must be given as separate shape filters");
            }
         }
      }
      if (mainNodeShape) {
         return mainNodeShape;
      } else {
         throw new Error("No main SHACL Node Shapes found in given shape filter");
      }
   } else {
      throw new Error("No SHACL Node Shapes found in given shape filter");
   }
}

export function sdsify(
   input: Stream<string | Quad[]>,
   output: Writer<string>,
   streamNode: Term,
   timestampPath?: Term,
   shapeFilters?: string[],
) {
   input.data(async (input) => {
      const dataStore = new Store(maybe_parse(input));
      console.log("[sdsify] Got input with", dataStore.size, "quads");
      const members: Array<{ [id: string]: Quad[] }> = [];
      const t0 = new Date();

      if (shapeFilters) {
         console.log("[sdsify] Extracting SDS members based on given shape(s)");

         for (const rawShape of shapeFilters) {
            const shapeStore = new Store();
            shapeStore.addQuads(new Parser().parse(rawShape));
            // Initialize shape extractor
            const shapeExtractor = new CBDShapeExtractor(shapeStore);
            // Find main node shape and target class
            const mainNodeShape = extractMainNodeShape(shapeStore);
            const targetClass = shapeStore.getObjects(mainNodeShape, SHACL.targetClass, null)[0];

            if (!targetClass) throw new Error("Given main node shape does not define a sh:targetClass");

            // Execute the CBDShapeExtractor over every targeted instance of the given shape
            for (const entity of dataStore.getSubjects(RDF.type, targetClass, null)) {
               members.push({ [entity.value]: await shapeExtractor.extract(dataStore, entity, mainNodeShape) });
            }
         }
      } else {
         // Extract members based on a Concise Bound Description (CBD)
         const cbdExtractor = new CBDShapeExtractor();

         for (const sub of dataStore.getSubjects(null, null, null)) {
            if (sub instanceof NamedNode) {
               members.push({ [sub.value]: await cbdExtractor.extract(dataStore, sub) });
            }
         }
      }

      console.log(`[sdsify] Members extracted in ${new Date().getTime() - t0.getTime()} ms`);

      // Sort members based on the given timestamp value (if any) to avoid out of order writing issues downstream
      if (timestampPath) {
         members.sort((a, b) => {
            const ta = new Date(dataStore.getObjects(Object.keys(a)[0], timestampPath, null)[0].value).getTime();
            const tb = new Date(dataStore.getObjects(Object.keys(b)[0], timestampPath, null)[0].value).getTime();
            return ta - tb;
         });
      }

      let membersCount = 0;

      // Create a unique transaction ID based on the data content and the current system time
      const hash = createHash("md5");
      const TRANSACTION_ID = hash.update(new NWriter()
         .quadsToString(dataStore.getQuads(null, null, null, null)))
         .digest("hex") + "_" + new Date().toISOString();

      for (const obj of members) {
         const key = Object.keys(obj)[0];
         const quads = obj[key];
         const blank = blankNode();

         quads.push(
            DataFactory.quad(blank, SDS.terms.payload, namedNode(key)),
            DataFactory.quad(blank, SDS.terms.stream, <Quad_Object>streamNode),
            // This is not standardized (yet)
            DataFactory.quad(namedNode(key), LDES.terms.custom("transactionId"), DataFactory.literal(TRANSACTION_ID))
         );

         if (membersCount === Object.keys(members).length - 1) {
            // Annotate last member of a transaction
            quads.push(
               // This is not standardized (yet)
               DataFactory.quad(
                  namedNode(key), 
                  LDES.terms.custom("isLastOfTransaction"), 
                  DataFactory.literal("true", XSD.terms.custom("boolean"))
               )
            );
         }

         await output.push(new NWriter().quadsToString(quads));
         membersCount += 1;
      }

      console.log(`[sdsify] successfully pushed ${membersCount} members in ${new Date().getTime() - t0.getTime()} ms`);
   });

   input.on("end", async () => {
      console.log("[sdsify] input channel was closed down");
      await output.end();
   });
}
