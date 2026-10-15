import {
    AddRelation,
    Bucketizer,
    RTreeFragmentation,
    RemoveRelation,
} from "./index";
import { Bucket, GEO, RdfThing, Record } from "../utils";
import { BasicLensM, Cont } from "rdf-lens";
import { Term } from "@rdfjs/types";
import { TREE, XSD } from "@treecg/types";
import { DataFactory } from "n3";
import { getLoggerFor } from "../utils/logUtil";
import { wktToGeoJSON, geojsonToWKT } from "@terraformer/wkt";
import { calculateBounds } from "@terraformer/spatial";
import type { Point, Polygon } from "geojson";

const { literal } = DataFactory;

type MBR = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
};

interface RTreeLeafEntry {
    id: string; // Member ID
    mbr: MBR;
}

interface RTreeNode {
    id: string; // Bucket Key
    mbr: MBR;
    children: (RTreeNode | RTreeLeafEntry)[];
    isLeaf: boolean;
    parent?: string; // Parent Bucket Key
}

function isRTreeNode(x: unknown): x is RTreeNode {
    return typeof x === "object" && x !== null && "isLeaf" in x;
}

export default class RTreeBucketizer implements Bucketizer {
    protected readonly logger = getLoggerFor(this);

    private readonly wktPath?: BasicLensM<
        Cont,
        { value: string; literal?: Term }
    >;
    private readonly wktPathQuads?: RdfThing;
    private readonly latitudePath?: BasicLensM<Cont, string>;
    private readonly latitudePathQuads?: RdfThing;
    private readonly longitudePath?: BasicLensM<Cont, string>;
    private readonly longitudePathQuads?: RdfThing;
    private readonly pageSize: number;

    private root: RTreeNode;
    private nodes: Map<string, RTreeNode> = new Map();
    private crs?: string;

    constructor(config: RTreeFragmentation, save?: string) {
        if (config.wktPath) {
            this.wktPath = config.wktPath.mapAll((x) => ({
                value: x.id.value,
                literal: x.id,
            }));
            this.wktPathQuads = config.wktPathQuads;
        }

        if (config.latitudePath && config.longitudePath) {
            this.latitudePath = config.latitudePath.mapAll((x) => x.id.value);
            this.latitudePathQuads = config.latitudePathQuads;
            this.longitudePath = config.longitudePath.mapAll((x) => x.id.value);
            this.longitudePathQuads = config.longitudePathQuads;
        }

        if (!this.wktPath && (!this.latitudePath || !this.longitudePath)) {
            throw new Error(
                "RTreeBucketizer requires either a WKT fragmentation path or both latitude and longitude paths.",
            );
        }

        this.pageSize = config.pageSize;

        if (save) {
            const parsed = JSON.parse(save);
            this.root = this.reviveNode(parsed.root);
            this.crs = parsed.crs;
        } else {
            const mbr = {
                minX: Infinity,
                minY: Infinity,
                maxX: -Infinity,
                maxY: -Infinity,
            };
            this.root = {
                id: this.generateId(mbr),
                mbr,
                children: [],
                isLeaf: true,
            };
            this.nodes.set(this.root.id, this.root);
        }
    }

    private reviveNode(node: RTreeNode): RTreeNode {
        this.nodes.set(node.id, node);
        node.children = node.children.map((child) => {
            if (isRTreeNode(child)) {
                const revived = this.reviveNode(child);
                revived.parent = node.id;
                return revived;
            }
            return child;
        });
        return node;
    }

    private generateId(mbr: MBR): string {
        if (mbr.minX === Infinity) {
            return "root";
        }
        return `${mbr.minX}_${mbr.minY}__${mbr.maxX}_${mbr.maxY}`;
    }

    /**
     * Entry point for bucketizing a record.
     * Extracts geospatial values from the record, updates the R-Tree, and returns assigned buckets.
     */
    bucketize(
        record: Record,
        getBucket: (key: string, root?: boolean, keyIsId?: boolean) => Bucket,
        addRelation: AddRelation,
        removeRelation: RemoveRelation,
    ): Bucket[] {
        const mbrs: MBR[] = [];

        if (this.wktPath) {
            const values = this.wktPath.execute(record.data);
            for (const value of values) {
                if (value.literal) {
                    const mbr = this.parseWKT(value.literal.value);
                    if (mbr) mbrs.push(mbr);
                }
            }
        }

        if (this.latitudePath && this.longitudePath) {
            const lats = this.latitudePath.execute(record.data);
            const longs = this.longitudePath.execute(record.data);

            if (lats.length > 0 && longs.length > 0) {
                const lat = parseFloat(lats[0]);
                const long = parseFloat(longs[0]);
                if (!isNaN(lat) && !isNaN(long)) {
                    mbrs.push({
                        minX: long,
                        minY: lat,
                        maxX: long,
                        maxY: lat,
                    });
                }
            }
        }

        const out: Bucket[] = [];

        for (const mbr of mbrs) {
            // 1. Insert the member into the R-Tree structure
            this.insert(
                { id: record.data.id.value, mbr },
                getBucket,
                addRelation,
                removeRelation,
            );

            // 2. Locate the leaf node (bucket) where the member was placed
            const leaf = this.findLeafForMember(
                this.root,
                record.data.id.value,
            );
            if (leaf) {
                out.push(getBucket(leaf.id, leaf.id === this.root.id));
            }
        }

        return out;
    }

    /**
     * Recursively searches for the leaf node that contains the specified member ID.
     * This is needed because a split might have moved the member from its original insertion point.
     */
    private findLeafForMember(
        node: RTreeNode,
        memberId: string,
    ): RTreeNode | null {
        if (node.isLeaf) {
            if (node.children.some((c) => !isRTreeNode(c) && c.id === memberId))
                return node;
            return null;
        }
        for (const child of node.children as RTreeNode[]) {
            const found = this.findLeafForMember(child, memberId);
            if (found) return found;
        }
        return null;
    }

    /**
     * Standard R-Tree insertion logic.
     * Finds the best leaf, appends the entry, and handles overflow if pageSize is exceeded.
     */
    private insert(
        entry: RTreeLeafEntry,
        getBucket: (key: string, root?: boolean, keyIsId?: boolean) => Bucket,
        addRelation: AddRelation,
        removeRelation: RemoveRelation,
    ): void {
        // 1. Find the leaf node that requires the least enlargement to fit this entry
        const leaf = this.findBestLeaf(this.root, entry.mbr);

        // 2. Add the entry to the leaf
        leaf.children.push(entry);
        this.extendMBR(leaf.mbr, entry.mbr);

        // 3. If the leaf is full, split it; otherwise, propagate MBR updates upwards
        if (leaf.children.length > this.pageSize) {
            this.handleOverflow(leaf, getBucket, addRelation, removeRelation);
        } else {
            this.updateMBRsUp(leaf, getBucket, addRelation, removeRelation);
        }
    }

    /**
     * Chooses the best leaf node for a new MBR.
     * Selects the child node whose MBR requires the minimum area enlargement.
     */
    private findBestLeaf(node: RTreeNode, mbr: MBR): RTreeNode {
        if (node.isLeaf) return node;
        let minEnlargement = Infinity;
        let bestChild: RTreeNode | null = null;

        for (const child of node.children as RTreeNode[]) {
            const enlargement = this.getEnlargement(child.mbr, mbr);
            if (enlargement < minEnlargement) {
                minEnlargement = enlargement;
                bestChild = child;
            } else if (enlargement === minEnlargement) {
                if (
                    !bestChild ||
                    this.getArea(child.mbr) < this.getArea(bestChild.mbr)
                ) {
                    bestChild = child;
                }
            }
        }
        return this.findBestLeaf(bestChild!, mbr);
    }

    /**
     * Manages node overflow by splitting the node and propagating the split upwards.
     * Grows the tree height if the root overflows.
     */
    private handleOverflow(
        node: RTreeNode,
        getBucket: (key: string, root?: boolean, keyIsId?: boolean) => Bucket,
        addRelation: AddRelation,
        removeRelation: RemoveRelation,
    ) {
        // 1. Split children into two groups using the Quadratic Split algorithm
        const [group1, group2] = this.quadraticSplit(node.children);

        // 2. Create a new peer node for the second group
        const mbr2 = this.calculateMBR(group2);
        const newNode: RTreeNode = {
            id: this.generateId(mbr2),
            mbr: mbr2,
            children: group2,
            isLeaf: node.isLeaf,
        };
        this.nodes.set(newNode.id, newNode);

        // 3. Update the existing node with the first group
        node.children = group1;
        node.mbr = this.calculateMBR(group1);

        // 4. Update parent pointers for children (if children are nodes)
        if (!node.isLeaf) {
            for (const c of group1 as RTreeNode[]) c.parent = node.id;
            for (const c of group2 as RTreeNode[]) c.parent = newNode.id;
        }

        if (node.id === this.root.id) {
            // Special Case: Root Split
            // The prefix bucket (root) MUST remain the tree entry point.
            // We create two new internal nodes as children of the root.
            const child1: RTreeNode = {
                id: this.generateId(node.mbr),
                mbr: { ...node.mbr },
                children: [...node.children],
                isLeaf: node.isLeaf,
                parent: node.id,
            };
            this.nodes.set(child1.id, child1);
            if (!child1.isLeaf) {
                for (const c of child1.children as RTreeNode[])
                    c.parent = child1.id;
            }

            const child2 = newNode;
            child2.parent = node.id;

            // Root now contains only the two new children
            node.children = [child1, child2];
            node.isLeaf = false;
            node.mbr = this.calculateMBR(node.children);

            this.createRelation(node.id, child1, getBucket, addRelation);
            this.createRelation(node.id, child2, getBucket, addRelation);
        } else {
            // Propagate split to parent
            const parent = this.nodes.get(node.parent!)!;
            newNode.parent = parent.id;
            parent.children.push(newNode);

            // Re-emit parent's relation for the current node (MBR might have shrunk)
            this.updateRelation(
                parent,
                node,
                getBucket,
                addRelation,
                removeRelation,
            );
            // Emit new relation for the peer node
            this.createRelation(parent.id, newNode, getBucket, addRelation);

            // Handle cascading overflow if parent is now full
            if (parent.children.length > this.pageSize) {
                this.handleOverflow(
                    parent,
                    getBucket,
                    addRelation,
                    removeRelation,
                );
            } else {
                this.updateMBRsUp(
                    parent,
                    getBucket,
                    addRelation,
                    removeRelation,
                );
            }
        }
    }

    /**
     * Emits a GeospatiallyContainsRelation between buckets.
     */
    private createRelation(
        parentId: string,
        child: RTreeNode,
        getBucket: (key: string, root?: boolean, keyIsId?: boolean) => Bucket,
        addRelation: AddRelation,
    ) {
        const parentBucket = getBucket(parentId, parentId === this.root.id);
        const childBucket = getBucket(child.id, false);

        if (this.wktPath) {
            addRelation(
                parentBucket,
                childBucket,
                TREE.terms.GeospatiallyContainsRelation,
                literal(this.mbrToWKT(child.mbr), GEO.terms.wktLiteral),
                this.wktPathQuads,
            );
        }

        if (this.latitudePath && this.longitudePath) {
            // Latitude
            addRelation(
                parentBucket,
                childBucket,
                TREE.terms.GreaterThanOrEqualToRelation,
                literal(child.mbr.minY.toString(), XSD.terms.custom("double")),
                this.latitudePathQuads,
            );
            addRelation(
                parentBucket,
                childBucket,
                TREE.terms.LessThanOrEqualToRelation,
                literal(child.mbr.maxY.toString(), XSD.terms.custom("double")),
                this.latitudePathQuads,
            );

            // Longitude
            addRelation(
                parentBucket,
                childBucket,
                TREE.terms.GreaterThanOrEqualToRelation,
                literal(child.mbr.minX.toString(), XSD.terms.custom("double")),
                this.longitudePathQuads,
            );
            addRelation(
                parentBucket,
                childBucket,
                TREE.terms.LessThanOrEqualToRelation,
                literal(child.mbr.maxX.toString(), XSD.terms.custom("double")),
                this.longitudePathQuads,
            );
        }
    }

    /**
     * Updates an existing relation by removing the stale one and adding a new one.
     */
    private updateRelation(
        parent: RTreeNode,
        child: RTreeNode,
        getBucket: (key: string, root?: boolean, keyIsId?: boolean) => Bucket,
        addRelation: AddRelation,
        removeRelation: RemoveRelation,
    ) {
        const parentBucket = getBucket(parent.id, parent.id === this.root.id);
        const childBucket = getBucket(child.id, false);

        const relationTypes = [
            TREE.terms.GeospatiallyContainsRelation,
            TREE.terms.GreaterThanOrEqualToRelation,
            TREE.terms.LessThanOrEqualToRelation,
        ];

        for (const type of relationTypes) {
            removeRelation(
                parentBucket,
                childBucket,
                type,
                undefined,
                undefined,
            );
        }
        this.createRelation(parent.id, child, getBucket, addRelation);
    }

    /**
     * Propagates MBR changes up to the root.
     * Updates relations between parent-child nodes if the MBR changed.
     */
    private updateMBRsUp(
        node: RTreeNode,
        getBucket: (key: string, root?: boolean, keyIsId?: boolean) => Bucket,
        addRelation: AddRelation,
        removeRelation: RemoveRelation,
    ) {
        let current = node;
        while (current.parent) {
            const parent = this.nodes.get(current.parent)!;
            const oldMBR = { ...parent.mbr };
            parent.mbr = this.calculateMBR(parent.children);
            if (this.mbrEquals(oldMBR, parent.mbr)) break;

            if (parent.parent) {
                const grandParent = this.nodes.get(parent.parent)!;
                this.updateRelation(
                    grandParent,
                    parent,
                    getBucket,
                    addRelation,
                    removeRelation,
                );
            }
            current = parent;
        }
    }

    /**
     * Guttman's Quadratic Split algorithm.
     * Divides children into two sets to minimize total area enlargement.
     */
    private quadraticSplit(
        children: (RTreeNode | RTreeLeafEntry)[],
    ): [(RTreeNode | RTreeLeafEntry)[], (RTreeNode | RTreeLeafEntry)[]] {
        if (children.length < 2) return [[...children], []];

        // 1. Pick Seeds: Find the two children that waste the most area if put in the same node
        let maxWaste = -Infinity;
        let seedIndices: [number, number] = [0, 1];

        for (let i = 0; i < children.length; i++) {
            for (let j = i + 1; j < children.length; j++) {
                const combined = this.getCombinedMBR(
                    children[i].mbr,
                    children[j].mbr,
                );
                const waste =
                    this.getArea(combined) -
                    this.getArea(children[i].mbr) -
                    this.getArea(children[j].mbr);
                if (waste > maxWaste) {
                    maxWaste = waste;
                    seedIndices = [i, j];
                }
            }
        }

        const group1 = [children[seedIndices[0]]];
        const group2 = [children[seedIndices[1]]];
        let mbr1 = children[seedIndices[0]].mbr;
        let mbr2 = children[seedIndices[1]].mbr;

        const remaining = children.filter(
            (_, i) => i !== seedIndices[0] && i !== seedIndices[1],
        );

        // 2. Distribute Remaining: Assign each entry to the group that needs least enlargement
        while (remaining.length > 0) {
            let bestEntryIndex = 0;
            let maxDiff = -1;
            let bestGroup = 1;

            for (let i = 0; i < remaining.length; i++) {
                const e1 = this.getEnlargement(mbr1, remaining[i].mbr);
                const e2 = this.getEnlargement(mbr2, remaining[i].mbr);
                const diff = Math.abs(e1 - e2);
                if (diff > maxDiff) {
                    maxDiff = diff;
                    bestEntryIndex = i;
                    bestGroup = e1 < e2 ? 1 : 2;
                }
            }

            const entry = remaining.splice(bestEntryIndex, 1)[0];
            if (bestGroup === 1) {
                group1.push(entry);
                mbr1 = this.getCombinedMBR(mbr1, entry.mbr);
            } else {
                group2.push(entry);
                mbr2 = this.getCombinedMBR(mbr2, entry.mbr);
            }
        }

        return [group1, group2];
    }

    /**
     * Computes the MBR containing all children using spatial library.
     */
    private calculateMBR(children: (RTreeNode | RTreeLeafEntry)[]): MBR {
        if (children.length === 0) {
            return {
                minX: Infinity,
                minY: Infinity,
                maxX: -Infinity,
                maxY: -Infinity,
            };
        }
        const bbox = calculateBounds({
            type: "GeometryCollection",
            geometries: children.map((c) => this.mbrToGeoJSON(c.mbr)),
        });
        return {
            minX: bbox[0],
            minY: bbox[1],
            maxX: bbox[2],
            maxY: bbox[3],
        };
    }

    private getCombinedMBR(a: MBR, b: MBR): MBR {
        const bbox = calculateBounds({
            type: "GeometryCollection",
            geometries: [this.mbrToGeoJSON(a), this.mbrToGeoJSON(b)],
        });
        return {
            minX: bbox[0],
            minY: bbox[1],
            maxX: bbox[2],
            maxY: bbox[3],
        };
    }

    private mbrEquals(a: MBR, b: MBR): boolean {
        return (
            a.minX === b.minX &&
            a.minY === b.minY &&
            a.maxX === b.maxX &&
            a.maxY === b.maxY
        );
    }

    private getEnlargement(a: MBR, b: MBR): number {
        const combined = this.getCombinedMBR(a, b);
        return this.getArea(combined) - this.getArea(a);
    }

    private getArea(mbr: MBR): number {
        if (mbr.minX === Infinity) return 0;
        const w = mbr.maxX - mbr.minX;
        const h = mbr.maxY - mbr.minY;
        return w * h;
    }

    /**
     * Parses a WKT literal into an MBR using spatial library.
     */
    private parseWKT(wkt: string): MBR | null {
        try {
            const match = wkt.trim().match(/^(<[^>]+>)\s*(.*)$/);
            const strippedWkt = match ? match[2] : wkt.trim();
            if (match && !this.crs) {
                this.crs = match[1];
            }
            const geojson = wktToGeoJSON(strippedWkt);
            const bbox = calculateBounds(geojson);
            return {
                minX: bbox[0],
                minY: bbox[1],
                maxX: bbox[2],
                maxY: bbox[3],
            };
        } catch (e: unknown) {
            this.logger.error(
                `Failed to parse WKT: ${wkt}. Error: ${(e as Error).message}`,
            );
            return null;
        }
    }

    private mbrToGeoJSON(mbr: MBR): Point | Polygon {
        if (mbr.minX === mbr.maxX && mbr.minY === mbr.maxY) {
            return {
                type: "Point",
                coordinates: [mbr.minX, mbr.minY],
            };
        }
        return {
            type: "Polygon",
            coordinates: [
                [
                    [mbr.minX, mbr.minY],
                    [mbr.maxX, mbr.minY],
                    [mbr.maxX, mbr.maxY],
                    [mbr.minX, mbr.maxY],
                    [mbr.minX, mbr.minY],
                ],
            ],
        };
    }

    private mbrToWKT(mbr: MBR): string {
        const wkt = geojsonToWKT(this.mbrToGeoJSON(mbr));
        return this.crs ? `${this.crs} ${wkt}` : wkt;
    }

    private extendMBR(a: MBR, b: MBR) {
        const combined = this.getCombinedMBR(a, b);
        a.minX = combined.minX;
        a.minY = combined.minY;
        a.maxX = combined.maxX;
        a.maxY = combined.maxY;
    }

    save(): string {
        return JSON.stringify({
            root: this.root,
            crs: this.crs,
        });
    }
}
