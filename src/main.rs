use clap::{Arg, App};
use glob::glob;
use rayon::prelude::*;
use serde_json::from_reader;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::iter::Iterator;
use std::path::{Path, PathBuf};
use std::sync::{RwLock};
use std::time::{Instant};
use unbytify::unbytify;

type Corpus = Vec<          // Documents
    Vec<                    // Sentences
        Vec<                // Words
            (
                Vec<char>,  // character
                u8          // tag
            )
        >
    >
>;

/// Vectorize all `corpuses` in given paths using pre-defined `map`.
/// If char in corpus is not exist in the map, it'll use `init` as 
/// vectorized value then assign new `char` and `init` into map.
/// It then increase init by 1.
/// If corpus contain non-Thai characters and it need to be vectorized
/// then it has to be in char_include_list.
/// 
/// # Parameter
/// - `buf_size` - Buffer size when reading corpus.
/// - `char_include_list` - Slice of non-Thai character to be vectorized.
/// - `corpuses` - Slice of PathBuf that point to corpus file.
/// - `init` - An RwLock that store u8. An unsign int value that will be used
/// on char that has no map inside `map` table yet.
/// - `map` - A HashMap that map a character to unsign int.
/// 
/// # Return
/// Vec contains a pair of u8. The first u8 is vectorized char. The second u8 is tag.
fn vectorize(buf_size: usize, char_include_list: &[char], corpuses: &[PathBuf], init: &RwLock<u8>, map: &RwLock<HashMap<char, u8>>) -> Vec<(u8, u8)> {
    corpuses.par_iter().flat_map(|f| {
        println!("Parsing:{}", f.display());
        let corpus_file = BufReader::with_capacity(buf_size,File::open(f).unwrap());
        let json: Corpus = from_reader(corpus_file).unwrap();
        let tagged_file: Vec<(u8, u8)> = json.iter().flat_map(|doc| {
            let tagged_doc: Vec<(u8, u8)> = doc.iter().flat_map(|sentence| {
                let tagged_sentence: Vec<(u8, u8)> = sentence.iter().flat_map(|(word, tag)| {
                    let mut tagged_chars: Vec<(u8, u8)> = word.iter().map(|ch| {
                        let codepoint = *ch as u32;
                        if (codepoint < 0x0E01 || codepoint > 0x0E7F) && !char_include_list.contains(ch) {
                            return (0, 0)
                        }

                        {
                            let map = map.read().unwrap();

                            if let Some(v) = map.get(ch) {
                                return (*v, 0)
                            }
                        }
                        let mut map = map.write().unwrap();
                        let mut v = init.write().unwrap();
                        map.insert(*ch, *v);

                        *v += 1;

                        return (*v - 1, 0)
                    }).collect();
                    if let Some((_, ref mut l)) = tagged_chars.last_mut() {
                        *l = *tag;   
                    }
                    tagged_chars
                }).collect();

                tagged_sentence
            }).collect();

            tagged_doc
        }).collect();

        tagged_file
    }).collect()
}

fn get_unique_vecs_idx(gram: u8, raw: &[u8]) -> Vec<usize> {
    let g = gram as usize;
    let len = raw.len() - g;
    let mut flatten : Vec<Vec<u8>> = (0..len).into_par_iter().map(|i| {
        (0..g).into_iter().map(|j| raw[i + j]).collect()
    }).collect();
    flatten.sort_unstable();
    
    let unique = vec![0];
    let flatten_1 = &flatten[1..];
    let flatten_0 = &flatten[..(flatten.len() - 1)];
    let matcher = flatten_0.iter().zip(flatten_1.iter());
    let unique: Vec<usize> = unique.into_iter()
            .chain(
                matcher.enumerate()
                    .filter_map(|(i, (f_0, f_1))| {
                        if f_0.eq(f_1) {
                            None
                        } else {
                            Some(i + 1)
                        }
                    }
                )
            ).collect();

    // unique.append(&mut remain);

    unique
}

fn main() {
    let matches = App::new("BEST corpus analysis")
                    .version("0.0.1")
                    .author("Nattapong Sirilappanich")
                    .about("Analyze BEST corpus by various factor specified in paramter")
                    .arg(Arg::with_name("gram")
                                .short("g")
                                .long("gram")
                                .value_name("NUMBER")
                                .takes_value(true)
                                .required(true)
                                .help("Number of gram to be analyzed. For example, 3")
                                .validator(|n| {
                                    let n = n.parse::<u8>().unwrap();
                                    if n == 0 {
                                        Err("NUMBER must be greater than 0".to_string())
                                    } else {
                                        Ok(())
                                    }
                                }))
                    .arg(Arg::with_name("corpus src")
                                .short("s")
                                .long("src")
                                .value_name("FILES")
                                .multiple(false)
                                .takes_value(true)
                                .required(true)
                                .min_values(1)
                                .help("Files storing corpus.")
                                .long_help(
"
Files that contains corpus. You may use glob style file path if
your path doesn't contain platform specific symbol or environment
variable. For example, it's OK on following cases:
    -s /home/john/**/corpus source/**/* ../another/dir/*.txt
These example will result in file not found:
    -s \"~/**/corpus\" '$HOME/**/another'
However, these example may be usable depending on your terminal functionalities:
    -s ~/**/corpus $HOME/some/dir/*
Note: In MS.Windows. It use %HOME% instead of $HOME.
The different between two cases is the absence of quote surround each path.
Quote make these path a string and delegate path resolve to app.
However, Rust glob cannot resolve OS dependent glob path.
Without quote, OS shell will resolve glob for the app.
If path is platform independent, it doesn't matter if there's any quote or not.
"
                                ))
                    .arg(Arg::with_name("output file")
                                .short("o")
                                .long("out")
                                .value_name("FILE")
                                .default_value("out.csv")
                                .takes_value(true)
                                .help("CSV file to store analyze result")
                                .validator(|path| {
                                    // validate if given output path already exist
                                    // and confirm user if it's ok to overwrite it.
                                    if Path::new(&path).exists() {
                                        println!("The file to store output already exist. Do you want to overwrite it (y/n) ?");
                                        let mut confirm = String::new();
                                        
                                        match std::io::stdin().read_line(&mut confirm) {
                                            Ok(_) => {
                                                let confirm = confirm.trim().to_lowercase();
                                                
                                                if !["y", "yes"].contains(&confirm.as_str()) {
                                                    return Err("The destination to store analyzed data already exist".to_owned())
                                                }
                                            },
                                            Err(err) => {
                                                panic!("{:?}", err)
                                            }
                                        }
                                    }

                                    Ok(())
                                }))
                    .arg(Arg::with_name("input buffer")
                                .short("ib")
                                .long("input-buffer")
                                .value_name("BUFFER_SIZE")
                                .default_value("16M")
                                .takes_value(true)
                                .help("Buffer size in bytes for corpus file reader. Default is 16MB."))
                    .arg(Arg::with_name("non-thai chars")
                                .short("cl")
                                .long("char-list-file")
                                .value_name("FILE")
                                .takes_value(true)
                                .help("A file that contains a non-Thai character per line")
                                .long_help(
"
A text file that contains a non-Thai character per line.
These are characters that will be vectorized into unique
number. 
"
                                ))
                    .get_matches();
    let gram = matches.value_of("gram").unwrap().parse::<u8>().unwrap();
    let sources = matches.values_of("corpus src").unwrap();
    let out_path = matches.value_of("output file").unwrap();
    let input_buffer_size = unbytify(matches.value_of("input buffer").unwrap()).unwrap() as usize;
    let mut char_include_list = match matches.value_of("non-thai chars") {
        Some(path) => {
            let reader = BufReader::new(File::open(path).expect("Invalid char-list-file path"));
            reader.lines().into_iter().map(|line| {
                let l = line.unwrap();
                // The only case of unwrap fail is empty line
                // We treat it as user expected to have \n added.
                l.chars().next().unwrap_or('\n')
            }).collect()
        },
        None => {
            vec![]
        }
    };

    char_include_list.sort_unstable();
    char_include_list.dedup();

    println!("{}-gram", gram);
    println!("Total {} source files", sources.len());
    println!("Input buffer: {} bytes", input_buffer_size);
    println!("Total non-Thai characters to be included is {} chars", char_include_list.len());
    // glob all the path specified by user
    let corpuses = sources.map(|s| {
        glob(s).unwrap().map(|g| g.unwrap())
    }).flatten().collect::<Vec<PathBuf>>();
    println!("Store output to {}", out_path);
    let timer = Instant::now();
    let map = RwLock::new(HashMap::<char, u8>::new());
    let v = RwLock::new(1u8);

    let tagged_chars: Vec<(u8, u8)> = vectorize(input_buffer_size, &char_include_list, &corpuses, &v, &map);

    println!("Total parsing took {} s", timer.elapsed().as_secs());
    println!("Total {} characters in corpus", tagged_chars.len());
    println!("Total {} unique characters", *v.read().unwrap());

    let (vecs, labels): (Vec<u8>, Vec<u8>) = tagged_chars.iter().cloned().unzip();

    // n-gram analysis
    let timer = Instant::now();
    let unique_idx = get_unique_vecs_idx(gram, &vecs);
    println!("Total unique analysis time is {}s", timer.elapsed().as_secs());
    println!("Total {} unique {}-gram", unique_idx.len(), gram);
}
