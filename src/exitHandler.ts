function noOp() { };
export function Cleanup(callback: () => void | Promise<void>) {

    // attach user callback to the process event emitter
    // if no callback, it will still exit gracefully on Ctrl-C
    callback = callback || noOp;

    // do app specific cleaning before exiting
    process.on("exit", callback);
    const fn = async function () {
        await callback();
        process.exit(2);
    };

    // catch ctrl+c event and exit normally
    process.on("SIGINT", fn);
    // process.on("SIGKILL", fn)
    // process.on("SIGSTOP", fn)
    process.on("SIGQUIT", fn);
    // process.on("SIG", fn)

    //catch uncaught exceptions, trace, then exit normally
    process.on("uncaughtException", async function (e) {
        await callback();
        process.exit(99);
    });
};