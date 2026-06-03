/// Tunables for a `NamespaceLog`.
#[derive(Debug, Clone, Copy)]
pub struct LogConfig {
    /// Active file size hint in bytes. When `active.write_offset()` exceeds this
    /// threshold, call `NamespaceLog::rotate_active()` to seal the active file and
    /// open a fresh one. Rotation is operator-controlled and NOT automatic.
    pub rotate_threshold: u64,
    /// Size-tiered compaction fanout: a level is merged into the next once it
    /// holds this many runs. Higher = less write-amp, more space-amp. Default 8
    /// (the measured knee).
    pub fanout: usize,
    /// Value-separation threshold in bytes. Values >= this are stored in the
    /// content-addressed blob store instead of inline in the log, so compaction
    /// never re-uploads them. Default 128 KiB = one GlideFS block: below a block,
    /// a blob-per-value wastes space; at/above it, separation collapses write-amp.
    pub value_sep_threshold: usize,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            rotate_threshold: 1 << 30, // 1 GiB
            fanout: 8,
            value_sep_threshold: 128 * 1024, // 128 KiB = one GlideFS block
        }
    }
}
