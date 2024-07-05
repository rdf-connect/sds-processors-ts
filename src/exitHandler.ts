function noOp() {}

export function Cleanup(callback: () => void | Promise<void>) {
    // attach user callback to the process event emitter
    // if no callback, it will still exit gracefully on Ctrl-C
    callback = callback || noOp;

    // Make sure we only call the callback once.
    let callbackCalled = false;
    const fn = async function (event: string, code?: number) {
        if (!callbackCalled) {
            callbackCalled = true;
            console.log(
                `[Cleanup] Callback called on '${event}' with code '${code}'.`,
            );
            await callback();
        } else {
            console.log(
                `[Cleanup] Callback has already been called. Ignoring '${event}' with code '${code}'.`,
            );
        }
        if (code) {
            process.exit(code);
        }
    };

    // do app specific cleaning before exiting
    process.on("exit", async () => await fn("exit"));

    // catch ctrl+c event and exit normally
    process.on("SIGINT", async () => await fn("SIGINT", 2));
    // process.on("SIGKILL", fn)
    // process.on("SIGSTOP", fn)
    process.on("SIGQUIT", async () => await fn("SIGQUIT", 2));
    // process.on("SIG", fn)

    //catch uncaught exceptions, trace, then exit normally
    process.on(
        "uncaughtException",
        async () => await fn("uncaughtException", 99),
    );
}
