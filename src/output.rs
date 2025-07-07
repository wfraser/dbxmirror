use crate::CommonOptions;
use console::Term;
use hashbrown::HashMap;
use indicatif::{
    InMemoryTerm, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressFinish, ProgressStyle,
    TermLike,
};
use log::{Level, LevelFilter, Metadata, Record};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Mutex, MutexGuard, OnceLock};

pub static OUT: OnceLock<Output> = OnceLock::new();

pub struct Output {
    term: Term,
    mp: MultiProgress,
    bars: Mutex<HashMap<String, ProgressBar>>,
    overall: ProgressBar,
    debug: bool,
    total_files: AtomicUsize,
    finished_files: AtomicUsize,
}

fn bar_style() -> ProgressStyle {
    ProgressStyle::with_template("{wide_msg} {decimal_total_bytes:>11} {decimal_bytes_per_sec:>11} {eta:>4} [{bar}] {percent:>3}%")
        .unwrap()
        .progress_chars("=> ")
}

fn paths_bar_style() -> ProgressStyle {
    ProgressStyle::with_template("{msg} {pos} files").unwrap()
}

impl Output {
    pub fn init(opts: &CommonOptions) {
        let term = Term::stderr();

        let out = Self {
            term: term.clone(),
            mp: MultiProgress::with_draw_target(ProgressDrawTarget::term(term, 10)),
            bars: Mutex::new(HashMap::new()),
            overall: ProgressBar::hidden().with_style(bar_style()),
            debug: opts.debug,
            total_files: AtomicUsize::new(0),
            finished_files: AtomicUsize::new(0),
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

    /// Set the total expected size of all downloads.
    pub fn set_total(&self, files: usize, size: u64) {
        self.total_files.store(files, Relaxed);
        let finished = self.finished_files.load(Relaxed);
        self.overall
            .set_message(format!("Total ({finished}/{files})"));
        self.overall.set_length(size);
    }

    /// Remove one file (with given size) from the expected size of all downloads.
    pub fn dec_total(&self, size: u64) {
        let total = self.total_files.fetch_sub(1, Relaxed) + 1;
        let finished = self.finished_files.load(Relaxed);
        self.overall
            .set_message(format!("Total ({finished}/{total})"));
        self.overall.dec_length(size);
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

        let bar = bars.entry_ref(path).or_insert_with(|| {
            let bar = ProgressBar::new(size)
                .with_style(bar_style())
                .with_message(path.to_owned())
                .with_finish(ProgressFinish::AndClear);
            self.mp.insert_from_back(1, bar.clone());
            bar
        });

        self.overall.inc(cur - bar.position());
        bar.set_position(cur);

        if cur == size {
            self.finish_file(bars, path);
        }
    }

    /// Remove the progress bar for a path regardless of whether it's finished or not.
    /// Use this in case there's an error or something which will prevent it from completing.
    pub fn remove_bar(&self, path: &str) {
        let bars = self.bars.lock().unwrap();
        self.finish_file(bars, path);
    }

    fn finish_file(&self, mut bars: MutexGuard<HashMap<String, ProgressBar>>, path: &str) {
        let Some(bar) = bars.remove(path) else { return };

        // Unfortunately, abandoned bars get left where they are in the order, with all prints going
        // above them, and other active bars staying above or below them, so we have to resort to
        // shenanigans.

        // Unlink the bar from the multi bar draw target, render it to a fake one instead, then emit
        // it as a println so it stays in place.
        let fake = InMemoryTerm::new(self.term.height(), self.term.width());
        bar.set_draw_target(ProgressDrawTarget::term_like(Box::new(fake.clone())));
        bar.abandon();

        let mut txt = fake.contents();
        let path_len = prefix_len(&txt, path);
        if path_len < path.len() {
            // Path got truncated; print the whole path, followed by the bar on a new line with the
            // path there replaced with spaces.
            txt = path.to_owned() + "\n" + &" ".repeat(path_len) + &txt[path_len..];
        }
        self.mp.println(txt).unwrap();

        let total = self.total_files.load(Relaxed);
        let finished = self.finished_files.fetch_add(1, Relaxed) + 1;
        if total == finished {
            // Force the overall bar to get redrawn when we change its message, regardless of timers
            // and whatnot, since it's all going away very shortly and may not get this update drawn
            // at all otherwise
            self.overall.finish();
        }
        self.overall
            .set_message(format!("Total ({finished}/{total})"));
        if bars.is_empty() {
            self.mp.remove(&self.overall);
        }
    }

    pub fn paths_progress(&self) -> ProgressBar {
        let bar = ProgressBar::no_length()
            .with_style(paths_bar_style())
            .with_message("Fetching file listing...")
            .with_finish(ProgressFinish::AndClear);
        self.mp.insert(0, bar.clone());
        bar
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

        let bars = self.bars.lock().unwrap();
        if bars.is_empty() {
            eprintln!("{msg}");
        } else {
            self.mp.println(&msg).unwrap();
        }
    }

    fn flush(&self) {}
}

fn prefix_len(s: &str, prefix: &str) -> usize {
    let mut pfx_it = prefix.chars();
    match s.find(|c: char| match pfx_it.next() {
        Some(pc) => c != pc,
        None => true,
    }) {
        Some(pos) => pos,
        None => s.len(),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_prefix_len() {
        assert_eq!(prefix_len("abc", "abc"), 3);
        assert_eq!(prefix_len("abcd", "abc"), 3);
        assert_eq!(prefix_len("abcd", "xyz"), 0);
        assert_eq!(prefix_len("abc", "abcd"), 3);
    }
}
