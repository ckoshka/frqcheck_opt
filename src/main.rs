use nohash_hasher::IntSet;
use rayon::prelude::*;
use term_macros::*;
use std::io::Write;
use std::hash::{Hash, Hasher};
use fnv::FnvHasher;
use std::io::Read;

type Filename = String;
type Contexts = Vec<IntSet<u32>>;

fn hash_str(s: &str) -> u32 {
    let mut h = FnvHasher::with_key(0);
    s.hash(&mut h);
    h.finish() as u32
}

fn main() {
    tool! {
        args:
            - known_words_file: Filename;
            - desired_words_file: Filename;
            - sentences_file: Filename;
            - ctxs_file: Filename;
        ;

        body: || {
            let mkwords = |filename: String| open!(filename.as_str()).split("\n").map(hash_str).collect::<IntSet<_>>();
            let desired_words = mkwords(desired_words_file);
            let mut known_words = mkwords(known_words_file);
            desired_words.iter().for_each(|w| {
                known_words.insert(*w);
            });

            let sentences_mmap = mmap!(sentences_file);

            let mut nls: Vec<_> = vec![];
            let mut ctxs: Contexts = vec![];

            rayon::scope(|s| {
                s.spawn(|_| {
                    nls = sentences_mmap.par_iter()
                        .enumerate()
                        .filter(|(_, b)| **b == b'\n')
                        .map(|(i, _)| i)
                        .collect();
                });

                s.spawn(|_| {
                    let ctxs_mmap = mmap!(ctxs_file);
                    ctxs = rmp_serde::from_slice(&ctxs_mmap[..]).unwrap();
                });
            });

            let get_sentence = |line_number: usize| {
                let line_number = line_number - 1; // integer overflow occurs here
                let start = nls[line_number] + 1;
                let end = nls[line_number+1];
                &sentences_mmap[start..end]
            };

            // it's because dicer is ignoring the originals

            let valid_ids = ctxs
                .par_iter()
                .enumerate()
                .filter(|(_, ctx)| {
                    ctx.difference(&known_words).next().is_none() && ctx.intersection(&desired_words).next().is_some() && ctx.len() > 0
                })
                .filter_map(|(i, _)| {
                    if i > 0 {
                        Some((i, get_sentence(i)))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            let mut stdout = std::io::BufWriter::new(std::io::stdout());

            valid_ids.iter().for_each(|(_, sentence)| {
                stdout.write_all(sentence).unwrap();
                stdout.write_all(b"\n").unwrap();
            });

            stdout.write_all(b"\n").unwrap();

            stdout.flush().unwrap();
        }

    };
}
