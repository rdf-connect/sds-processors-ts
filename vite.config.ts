import { defineConfig } from "vitest/config";

export default defineConfig({
    resolve: {
        tsconfigPaths: true,
    },
    test: {
        deps: {
            optimizer: {
                ssr: {
                    enabled: true,
                    include: ["@rdfc/js-runner"],
                },
            },
        },
    },
});
