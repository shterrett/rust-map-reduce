use std::path::PathBuf;
use std::fs::{ File, OpenOptions };
use std::io::{ BufReader, BufRead, Write };
use std::sync::Arc;
use std::collections::HashMap;

extern crate mapreduce;
use mapreduce::master::Master;

fn split_input_file(working_directory: &PathBuf, input_file: &PathBuf) -> Vec<PathBuf> {
    let f = OpenOptions::new()
                        .read(true)
                        .open(&input_file)
                        .unwrap();

    let max_lines = 500;
    let mut count = 0;
    let mut input = vec![];
    let mut input_number = 1;
    for line in BufReader::new(f).lines().map(|l| l.unwrap_or("".to_string())) {
        input.push(line);
        count += 1;
        if count == max_lines {
            write_input_file(&working_directory, input_number, &mut input);
            input_number += 1;
            count = 0;
        }
    }

    let mut split = working_directory.clone();
    split.push(format!("input_{}", input_number));
    write_input_file(&working_directory, input_number, &mut input);

    (1..(input_number + 1)).map(|i| {
        let mut path = working_directory.clone();
        path.push(format!("input_{}", i));
        path
    })
    .collect()
}

fn write_input_file(working_directory: &PathBuf, index: usize, contents: &mut Vec<String>) {
    let mut path = working_directory.clone();
    path.push(format!("input_{}", index));
    let mut input_file = OpenOptions::new()
                                     .write(true)
                                     .create(true)
                                     .open(&path)
                                     .unwrap();
    for line in contents {
        let _ = input_file.write_all(line.as_bytes());
    }
}

fn map_fn(input: BufReader<File>) -> Vec<String> {
    let mut word_counts: HashMap<String, i32> = HashMap::new();
    for line in input.lines().map(|l| l.unwrap_or("".to_string())) {
        let words = line.split(" ");
        for word in words {
            *word_counts.entry(word.to_lowercase()).or_insert(0) += 1;
        }
    }

    let mut words = word_counts.keys().collect::<Vec<&String>>();
    words.sort();

    let mut a_to_e = vec![];
    let mut f_to_k = vec![];
    let mut l_to_q = vec![];
    let mut r_to_t = vec![];
    let mut u_to_z = vec![];

    for word in words {
        if let Some(first_letter) = word.chars().nth(0) {
            let line = format!("{}={}", word.clone(), word_counts.get(word).unwrap());
            match first_letter {
                'a' ... 'e' => a_to_e.push(line),
                'f' ... 'k' => f_to_k.push(line),
                'l' ... 'q' => l_to_q.push(line),
                'r' ... 't' => r_to_t.push(line),
                'u' ... 'z' => u_to_z.push(line),
                _ => {}
            }
        }
    }

    vec![a_to_e, f_to_k, l_to_q, r_to_t, u_to_z]
        .iter()
        .map(|group| {
            group.iter()
                 .fold("".to_string(), |mut contents, line| {
                     contents.push_str(&line);
                     contents.push_str("\n");
                     contents.clone()
                 })
                 .to_string()
        })
        .collect()
}

fn reduce_fn(input: Vec<BufReader<File>>) -> String {
    let mut word_counts: HashMap<String, i32> = HashMap::new();
    for file in input {
        for line in file.lines().map(|l| l.unwrap_or("".to_string())) {
            let mut word_count = line.split("=");
            let word = word_count.next().unwrap();
            let count = i32::from_str_radix(word_count.next().unwrap(), 10).unwrap();
            word_counts.insert(word.to_string(), count);
        }
    }

    let mut words = word_counts.keys().collect::<Vec<&String>>();
    words.sort();
    words.iter().fold("".to_string(), |mut results, &word| {
        results.push_str(&format!("{}={}\n", word, word_counts.get(word).unwrap()));
        results
    })
}

fn main() {
    let working_directory = PathBuf::from("./test-data/word_count_data");
    let mut book = working_directory.clone();
    book.push("war_of_the_worlds.txt");

    let input_files = split_input_file(&working_directory, &book);
    let map = Arc::new(map_fn);
    let reduce = Arc::new(reduce_fn);

    let master = Master::new(working_directory.clone(),
                             input_files,
                             map,
                             reduce
                            );
    let result_files = master.run(4);
}
