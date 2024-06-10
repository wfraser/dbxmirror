use crate::CommonOptions;
use console::Term;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressFinish, ProgressStyle};
use log::{Level, LevelFilter, Metadata, Record};
use std::collections::HashMap;
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

    pub fn download_progress(&self, path: &str, cur: u64, size: u64) {
        let mut bars = self.bars.lock().unwrap();
        if let Some(bar) = bars.get(path) {
            self.overall.inc(cur - bar.position());
            bar.set_position(cur);
            if cur == size {
                self.mp.println(path).unwrap();
            }
        } else {
            if bars.is_empty() {
                self.mp.add(self.overall.clone());
            }
            self.overall.inc_length(size);
            self.overall.inc(cur);

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
            bar.set_position(cur);
            bars.insert(path.to_owned(), bar);
        }

        if cur == size {
            bars.remove(path).unwrap();
            if bars.is_empty() {
                self.mp.remove(&self.overall);
            }
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
