use crate::db::Database;
use crate::StrExt;
use anyhow::anyhow;
use dropbox_sdk::files::{FileMetadata, Metadata};
use std::collections::HashMap;

pub enum Op {
    AddedFile(String, FileMetadata),
    DeletedFile(String),
    MovedFile { old_path: String, new_path: String },
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
    let mut folders = vec![];
    let mut deleted = vec![];

    'entries: for entry in entries {
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
            continue;
        }
        let path = path
            .ok_or_else(|| anyhow!("missing path_display field for Metadata entry"))?
            .strip_prefix_ignore_case(remote_root)
            .ok_or_else(|| {
                anyhow!(
                    "remote path {:?} doesn't start with root path {:?}",
                    path,
                    remote_root,
                )
            })?
            .to_owned();

        for (test, is_regex) in ignores {
            if *is_regex {
                todo!("regex support")
            } else if path.starts_with(test) {
                continue 'entries;
            }
        }

        match entry {
            Metadata::File(meta) => {
                let hash = meta
                    .content_hash
                    .clone()
                    .ok_or_else(|| anyhow!("missing content hash from server for {path:?}"))?;
                adds.entry(hash).or_default().push((path, meta));
            }
            Metadata::Folder(_) => folders.push(path),
            Metadata::Deleted(_) => deleted.push(path),
        }
    } // entries

    // Start by creating all folders.
    let mut ops = folders
        .into_iter()
        .map(Op::CreateFolder)
        .collect::<Vec<Op>>();

    // Look for matching adds for each delete and turn them into moves.
    for deleted_path in deleted {
        let Some((_mtime, hash)) = db.get_file(&deleted_path)? else {
            // Unknown file deted. This is probably a problem but let the caller deal with it.
            ops.push(Op::DeletedFile(deleted_path));
            continue;
        };

        if let Some(files) = adds.get_mut(&hash) {
            let (added_path, _meta) = files.pop().unwrap(); // not allowed to be empty
            ops.push(Op::MovedFile {
                old_path: deleted_path,
                new_path: added_path,
            });
            if files.is_empty() {
                adds.remove(&hash);
            }
        } else {
            ops.push(Op::DeletedFile(deleted_path));
        }
    }

    for (_hash, files) in adds {
        for (path, meta) in files {
            ops.push(Op::AddedFile(path, meta));
        }
    }

    Ok(ops)
}
