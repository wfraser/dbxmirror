use crate::db::Database;
use crate::StrExt;
use anyhow::anyhow;
use dropbox_sdk::files::{FileMetadata, Metadata};
use std::collections::{HashMap, HashSet};

pub enum Op {
    AddedFile(String, Box<FileMetadata>),
    DeletedFile(String),
    MovedFile {
        old_path: String,
        new_path: String,
        remote: Box<FileMetadata>,
    },
    CreateFolder(String),
}

pub fn list_folder_to_ops(
    entries: Vec<Metadata>,
    remote_root: &str,
    ignores: &[(String, bool)], // bool = is regex
    db: &Database,
) -> anyhow::Result<Vec<Op>> {
    assert!(
        remote_root.ends_with('/'),
        "remote_root must end in a slash"
    );
    let mut adds = HashMap::<String, Vec<(String, FileMetadata)>>::new();

    // First, filter out ignored paths, then find all added files and map them by content hash.
    let entries = entries
        .into_iter()
        .filter_map(|entry| -> Option<anyhow::Result<_>> {
            debug!("entry: {entry:?}");
            let path = match &entry {
                Metadata::File(f) => f.path_display.as_ref(),
                Metadata::Deleted(d) => d.path_display.as_ref(),
                Metadata::Folder(d) => d.path_display.as_ref(),
            };
            if path
                .map(|s| s.eq_ignore_case(&remote_root[..remote_root.len() - 1]))
                .unwrap_or(false)
            {
                // This is an entry for the root itself.
                return None;
            }

            let path = match path {
                Some(p) => p,
                None => {
                    return Some(Err(anyhow!(
                        "missing path_display field for Metadata entry"
                    )))
                }
            };
            let path = match path.strip_prefix_ignore_case(remote_root) {
                Some(p) => p.to_owned(),
                None => {
                    return Some(Err(anyhow!(
                        "remote path {:?} doesn't start with root path {:?}",
                        path,
                        remote_root,
                    )))
                }
            };

            for (test, is_regex) in ignores {
                if *is_regex {
                    todo!("regex support")
                } else if path.starts_with(test) {
                    return None;
                }
            }

            if let Metadata::File(meta) = &entry {
                let hash = match meta.content_hash.as_ref() {
                    Some(h) => h.to_owned(),
                    None => {
                        return Some(Err(anyhow!(
                            "missing content hash from server for {path:?}"
                        )))
                    }
                };
                adds.entry(hash)
                    .or_default()
                    .push((path.clone(), meta.clone()));
            }

            Some(Ok((path, entry)))
        })
        .collect::<anyhow::Result<Vec<(String, _)>>>()?;

    let mut move_dests = HashSet::new();

    let mut ops = vec![];
    for (path, entry) in entries {
        match entry {
            Metadata::File(meta) => {
                if move_dests.remove(&path) {
                    // already did a move for this path; no need for an add
                    continue;
                }
                ops.push(Op::AddedFile(path, Box::new(meta)));
            }
            Metadata::Folder(_) => ops.push(Op::CreateFolder(path)),
            Metadata::Deleted(_) => {
                if let Some((hash, files)) = db
                    .get_file(&path)?
                    .and_then(|(_mtime, hash)| adds.get_mut(&hash).map(|files| (hash, files)))
                {
                    let (added_path, meta) = files.pop().unwrap(); // not allowed to be empty
                    ops.push(Op::MovedFile {
                        old_path: path,
                        new_path: added_path.clone(),
                        remote: Box::new(meta),
                    });
                    move_dests.insert(added_path);
                    if files.is_empty() {
                        adds.remove(&hash);
                    }
                } else {
                    ops.push(Op::DeletedFile(path));
                }
            }
        }
    }

    Ok(ops)
}
