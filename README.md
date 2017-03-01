# Map Reduce

Local MapReduce framework. See `examples/word_count.rs` for a usage example.

```rust
use std::sync::Arc;
extern crate mapreduce;
use mapreduce::master::Master;

let working_directory = PathBuf::from("./test-data/word_count_data");
let mut book = working_directory.clone();
book.push("war_of_the_worlds.txt");
let input_files = split_input_file(&working_directory, &book);

// define a map and reduce function
let map = Arc::new(map_fn);
let reduce = Arc::new(reduce_fn);

let master = Master::new(working_directory.clone(),
                          input_files,
                          map,
                          reduce
                        );
// Number of independent workers desired
let _ = master.run(4);
```

The implementation runs a map and reduce function on a given set of input files.

The `map` function has type `Fn(BufReader<File>) -> Vec<String> + Send + Sync`

The `reduce` function has type `Fn(Vec<BufReader<File>>) -> String + Send + Sync`

The `Master` is given a vector of input files and a working directory as
`PathBuf`. It then spawns the requested number of workers and dispatches jobs
via a channel, listening for results on a second channel. The workers' interface
is purely in terms of `String` and `BufReader<File>`; the framework handles
reading and writing the files themselves. The format of the files is left to
convention between the `map` and `reduce` functions; the framework doesn't
enforce any particular format. All result files are written to the working
directory, and a vector of `PathBuf` for the results is returned fr`m `Master::run` - the caller can merge the files as they see fit.
