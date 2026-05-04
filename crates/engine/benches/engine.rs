use bytes::Bytes;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

static VAL: &[u8] = &[b'x'; 64];

fn main() {
    divan::main();
}

// RuntimeBuilder<FusionDriver> returns a platform-specific type (e.g.
// FusionRuntime<TimeDriver<LegacyDriver>>); macro avoids naming it.
macro_rules! make_rt {
    () => {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .unwrap()
    };
}

fn key(i: usize) -> Bytes {
    Bytes::from(format!("k{i:08}"))
}

// ── MemCache ──────────────────────────────────────────────────────────────────

mod cache {
    use std::hint::black_box;

    use beyond_kv_engine::cache::MemCache;
    use bytes::Bytes;

    use super::{VAL, key};

    #[divan::bench]
    fn insert(bencher: divan::Bencher) {
        let cache = MemCache::new(64 << 20);
        let mut i = 0usize;
        bencher.bench_local(move || {
            cache.insert(key(i), Bytes::from_static(VAL), None, None, 0, 0);
            i += 1;
        });
    }

    #[divan::bench]
    fn get_hit(bencher: divan::Bencher) {
        let cache = MemCache::new(64 << 20);
        for i in 0..1_000 {
            cache.insert(key(i), Bytes::from_static(VAL), None, None, 0, 0);
        }
        let lookup = key(500);
        bencher.bench_local(move || black_box(cache.get(&lookup, 0)));
    }

    #[divan::bench]
    fn get_miss(bencher: divan::Bencher) {
        let cache = MemCache::new(64 << 20);
        bencher.bench_local(move || black_box(cache.get(b"no-such-key", 0)));
    }
}

// ── ShardStore ────────────────────────────────────────────────────────────────

mod store {
    use std::hint::black_box;

    use beyond_kv_engine::store::ShardStore;
    use beyond_kv_engine::types::SetOptions;
    use bytes::Bytes;
    use tempfile::TempDir;

    use super::{VAL, key};

    #[divan::bench]
    fn set(bencher: divan::Bencher) {
        let tmp = TempDir::new().unwrap();
        let mut rt = make_rt!();
        let store = rt.block_on(ShardStore::open(tmp.path(), 64 << 20)).unwrap();
        let mut i = 0usize;
        bencher.bench_local(move || {
            rt.block_on(store.set(
                "default",
                &key(i),
                Bytes::from_static(VAL),
                SetOptions::default(),
            ))
            .unwrap();
            i += 1;
        });
    }

    #[divan::bench]
    fn get_warm(bencher: divan::Bencher) {
        let tmp = TempDir::new().unwrap();
        let mut rt = make_rt!();
        let store = rt.block_on(ShardStore::open(tmp.path(), 64 << 20)).unwrap();
        rt.block_on(store.set(
            "default",
            b"k",
            Bytes::from_static(VAL),
            SetOptions::default(),
        ))
        .unwrap();
        bencher.bench_local(move || black_box(rt.block_on(store.get("default", b"k")).unwrap()));
    }

    #[divan::bench]
    fn get_cold(bencher: divan::Bencher) {
        let tmp = TempDir::new().unwrap();
        let mut rt = make_rt!();
        {
            let warm = rt.block_on(ShardStore::open(tmp.path(), 64 << 20)).unwrap();
            for i in 0..1_000 {
                rt.block_on(warm.set(
                    "default",
                    &key(i),
                    Bytes::from_static(VAL),
                    SetOptions::default(),
                ))
                .unwrap();
            }
        }
        // memory_bytes=1 keeps L1 effectively empty; every read hits the log.
        let cold = rt.block_on(ShardStore::open(tmp.path(), 1)).unwrap();
        let lookup_keys: Vec<Bytes> = (0..1_000).map(key).collect();
        let mut i = 0usize;
        bencher.bench_local(move || {
            black_box(
                rt.block_on(cold.get("default", &lookup_keys[i % 1_000]))
                    .unwrap(),
            );
            i += 1;
        });
    }

    #[divan::bench(args = [1, 10, 100])]
    fn mget(bencher: divan::Bencher, n: usize) {
        let tmp = TempDir::new().unwrap();
        let mut rt = make_rt!();
        let store = rt.block_on(ShardStore::open(tmp.path(), 64 << 20)).unwrap();
        for i in 0..n {
            rt.block_on(store.set(
                "default",
                &key(i),
                Bytes::from_static(VAL),
                SetOptions::default(),
            ))
            .unwrap();
        }
        let keys: Vec<Bytes> = (0..n).map(key).collect();
        bencher.bench_local(move || {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            black_box(rt.block_on(store.mget("default", &refs)).unwrap());
        });
    }

    #[divan::bench(args = [1, 10, 100])]
    fn mset(bencher: divan::Bencher, n: usize) {
        let tmp = TempDir::new().unwrap();
        let mut rt = make_rt!();
        let store = rt.block_on(ShardStore::open(tmp.path(), 64 << 20)).unwrap();
        let pairs: Vec<(Bytes, Bytes)> =
            (0..n).map(|i| (key(i), Bytes::from_static(VAL))).collect();
        bencher.bench_local(move || {
            black_box(rt.block_on(store.mset("default", &pairs)).unwrap());
        });
    }
}
