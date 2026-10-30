// Building a kernel moves the App's job definitions, outbound hosts and cron
// entries into it, so a second kernel on the App it held would run no jobs
// and no cron (0.4.0 spec §1 item 1). The kernel keeps its App for life:
// the end state reads through `kernel.app()`, and going on means running the
// same kernel against another source, or restarting and rebuilding the app.
use charpente::core::io::Clock;
use charpente::kernel::Kernel;

fn second_kernel(kernel: Kernel, clock: Box<dyn Clock>) -> std::io::Result<Kernel> {
    Kernel::new(kernel.into_app(), clock)
}

fn main() {
    let _ = second_kernel;
}
