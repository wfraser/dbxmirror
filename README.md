dbxmirror
=========

`dbxmirror` lets you maintain a (read-only) mirror of a Dropbox folder tree and efficiently keep it
up to date.

"Why not just use `rclone`?" you may ask. The main difference is `dbxmirror` is stateful, in that
it keeps track of what it's fetched already, and instead of asking Dropbox for a full recursive
directory listing every time, only asks for updates since the last sync. On a large directory tree,
this is a pretty significant speed-up.

# Usage

1. Run `dbxmirror setup /root-path` in the destination directory. It will prompt for
   authentication, requiring a Dropbox API app key. `/root-path` is any path in your Dropbox,
   and may be `/` to sync the whole thing.

2. Optionally run `dbxmirror ignore add /some/other/path` to mark paths to exclude.

3. Run `dbxmirror pull` to make the local directory match Dropbox.

4. Later on you may run `dbxmirror check` to verify that the local state matches what is expected.

# Notes

Be advised that `dbxmirror` creates a file `.dbxmirror.db` in the root of the destinatiton local
directory, which is a sqlite database and contains your Dropbox authentication token, so protect it
accordingly.
