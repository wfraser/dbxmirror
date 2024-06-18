use crate::CommonOptions;
use console::Term;
use hashbrown::HashMap;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressFinish, ProgressStyle};
use log::{Level, LevelFilter, Metadata, Record};
use std::sync::{Mutex, OnceLock};

pub static OUT: OnceLock<Output> = OnceLock::new();

pub struct Output {
    mp: MultiProgress,
    bars: Mutex<HashMap<String, ProgressBar>>,
    overall: ProgressBar,
    debug: bool,
}

impl Output {
    pub fn init(opts: &CommonOptions) {
        let out = Self {
            mp: MultiProgress::with_draw_target(ProgressDrawTarget::term(Term::stderr(), 10)),
            bars: Mutex::new(HashMap::new()),
            overall: ProgressBar::hidden().with_style(
                ProgressStyle::with_template(
                    "{decimal_bytes_per_sec} :: {decimal_bytes}/{decimal_total_bytes}",
                )
                .unwrap(),
            ),
            debug: opts.debug,
        };

        out.overall.set_length(0);

        OUT.set(out)
            .unwrap_or_else(|_| panic!("output has already been set"));
        log::set_logger(OUT.get().unwrap()).expect("logger has already been set");

        if opts.verbose || opts.debug {
            log::set_max_level(LevelFilter::Debug);
        } else {
            log::set_max_level(LevelFilter::Info);
        }
    }

    /// Add to the total expected size of all downloads.
    pub fn inc_total(&self, size: u64) {
        self.overall.inc_length(size);
    }

    /// Update the progress bar for a path, and also the overall progress.
    ///
    /// If no progress bar for the path is shown yet, begin showing it. If it's the first bar, also
    /// begin showing the overall progress.
    ///
    /// If the download is complete (cur == size), stop showing it.
    ///
    /// If the set of shown bars becomes empty, hides the overall progress.
    pub fn download_progress(&self, path: &str, cur: u64, size: u64) {
        let mut bars = self.bars.lock().unwrap();

        if bars.is_empty() {
            self.mp.add(self.overall.clone());
        }

        let bar = bars
            .entry_ref(path)
            .or_insert_with(|| {
                let bar = ProgressBar::new(size)
                    .with_style(
                        ProgressStyle::with_template(
                            "[{bar:25}] {decimal_bytes}/{decimal_total_bytes} ({decimal_bytes_per_sec}) {wide_msg}")
                            .unwrap()
                            .progress_chars("=> "),
                    )
                    .with_message(path.to_owned())
                    .with_finish(ProgressFinish::AndClear);
                self.mp.insert_from_back(1, bar.clone());
                bar
            });

        self.overall.inc(cur - bar.position());
        bar.set_position(cur);

        if cur == size {
            self.mp.println(path).unwrap();
            bars.remove(path).unwrap();
            if bars.is_empty() {
                self.mp.remove(&self.overall);
            }
        }
    }

    /// Remove the progress bar for a path regardless of whether it's finished or not.
    /// Use this in case there's an error or something which will prevent it from completing.
    pub fn remove_bar(&self, path: &str) {
        let mut bars = self.bars.lock().unwrap();
        bars.remove(path);
        if bars.is_empty() {
            self.mp.remove(&self.overall);
        }
    }
}

impl log::Log for Output {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.debug || metadata.level() <= Level::Warn || metadata.target().starts_with("dbxmirror")
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let mut msg = match record.level() {
            Level::Error => console::style("ERROR").red(),
            Level::Warn => console::style(" WARN").yellow(),
            Level::Info => console::style(" INFO").green(),
            Level::Debug => console::style("DEBUG").blue(),
            Level::Trace => console::style("TRACE").dim(),
        }
        .to_string();
        msg.push_str(": ");
        msg.push_str(&format!("{}", record.args()));
        self.mp.println(&msg).unwrap();
    }

    fn flush(&self) {}
}
