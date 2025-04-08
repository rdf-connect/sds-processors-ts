import { getLoggerFor } from "./utils/logUtil";

function noOp() {}

const logger = getLoggerFor("ExitHandler");

export function handleExit(callback: () => void | Promise<void>) {
    // attach user callback to the process event emitter
    // if no callback, it will still exit gracefully on Ctrl-C
    callback = callback || noOp;

    // Make sure we only call the callback once.
    let callbackCalled = false;
    const fn = async function (event: string, code?: number, message?: Error) {
        if (!callbackCalled) {
            callbackCalled = true;
            logger.debug(
                `[handleExit] Callback called on '${event}' with code '${code}'.`,
            );
            if (message) {
                logger.error(
                    `[handleExit] Uncaught Exception: ${message.name} - ${message.message}`,
                );
                logger.debug(message.stack);
            }
            await callback();
        } else {
            logger.debug(
                `[handleExit] Callback has already been called. Ignoring '${event}' with code '${code}'.`,
            );
        }
        if (code && process.listeners(<NodeJS.Signals>event).length <= 0) {
            process.exit(code);
        }
    };

    // catch ctrl+c event and exit normally
    process.once("SIGINT", async () => await fn("SIGINT", 2));
    process.once("SIGTERM", async () => await fn("SIGTERM", 143));
    process.once("SIGBREAK", async () => await fn("SIGBREAK", 149));
    process.once(
        "uncaughtException",
        async (error: Error) => await fn("uncaughtException", 99, error),
    );
    process.once(
        "unhandledRejection",
        async (error: Error) => await fn("unhandledRejection", 99, error),
    );
}
