/// Tunables for a `NamespaceLog`.
#[derive(Debug, Clone, Copy)]
pub struct LogConfig {
    /// Active file size hint in bytes. When `active.write_offset()` exceeds this
    /// threshold, call `NamespaceLog::rotate_active()` to seal the active file and
    /// open a fresh one. Rotation is operator-controlled and NOT automatic.
    pub rotate_threshold: u64,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            rotate_threshold: 1 << 30, // 1 GiB
        }
    }
}
