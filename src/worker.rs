use std::io::BufReader;
use std::fs::{ File, OpenOptions };
use std::io::Write;
use std::path::PathBuf;

use chan::{ Sender, Receiver };

#[derive(Debug, PartialEq, Eq)]
enum Job {
    Map(PathBuf),
    Reduce(Vec<PathBuf>)
}

#[derive(Debug, PartialEq, Eq)]
enum JobResult {
    MapFinished(i32),
    ReduceFinished(i32)
}

struct Worker {
    id: i32,
    working_directory: PathBuf,
    map: Box<Fn(BufReader<File>) -> Vec<String> + Send>,
    reduce: Box<Fn(Vec<BufReader<File>>) -> String + Send>,
    job_queue: Receiver<Job>,
    results_queue: Sender<JobResult>
}

impl Worker {
    fn run(&self) {
        for job in self.job_queue.iter() {
            match job {
                Job::Map(path) => {
                    let results: Vec<String> = (self.map)(open_file(path));
                    let names = self.map_result_names(results.iter().len());
                    self.write_map_results(names, results);
                    self.results_queue.send(JobResult::MapFinished(self.id));
                }
                Job::Reduce(paths) => {
                }
            }
        }
    }

    fn map_result_names(&self, length: usize) -> Vec<PathBuf> {
        (1..length + 1).map(|i| {
                       let mut path = self.working_directory.clone();
                       path.push(format!("worker.{}.reduce.{}", self.id, i));
                       path
                   })
                   .collect::<Vec<PathBuf>>()
    }

    fn write_map_results(&self, names: Vec<PathBuf>, results: Vec<String>) {
        for (filename, result) in names.iter().zip(results.into_iter()) {
            let mut f = File::create(filename).unwrap();
            let _ = f.write_all(&result.as_bytes());
        }
    }
}

fn open_file(path: PathBuf) -> BufReader<File> {
    let f = OpenOptions::new()
                        .read(true)
                        .open(path)
                        .unwrap();
    BufReader::new(f)
}

#[cfg(test)]
mod test {
    use std::fs::OpenOptions;
    use std::io::{ Write, BufRead, BufReader };
    use std::fs::{ File, remove_file };
    use std::path::PathBuf;
    use std::thread;

    use chan;
    use chan::{ Sender, Receiver };

    use super::{ Worker,
                 Job,
                 JobResult
               };

    #[test]
    fn worker_maps_input_to_result_files() {
        let working_directory = PathBuf::from("./test-data/worker_maps_input_to_result_files");
        let mut map_file = working_directory.clone();
        map_file.push("input_file");

        fn map_fn(input: BufReader<File>) -> Vec<String> {
            vec!["1", "2", "3", "4"].iter().map(|s| s.to_string()).collect()
        };
        fn reduce_fn(input: Vec<BufReader<File>>) -> String {
            "1234".to_string()
        };

        let (work_send, work_recv) = chan::async();
        let (results_send, results_recv) = chan::async();

        let worker = Worker {
            id: 1,
            working_directory: working_directory.clone(),
            map: Box::new(map_fn),
            reduce: Box::new(reduce_fn),
            job_queue: work_recv,
            results_queue: results_send
        };

        thread::spawn(move ||
            worker.run()
        );

        work_send.send(Job::Map(map_file.clone()));
        let done = results_recv.recv();
        drop(work_send);
        drop(results_recv);

        assert_eq!(done, Some(JobResult::MapFinished(1)));
        let expected_files = vec!["worker.1.reduce.1",
                                  "worker.1.reduce.2",
                                  "worker.1.reduce.3",
                                  "worker.1.reduce.4"
                                 ];

        let contents = expected_files.iter()
                                     .flat_map(|name| {
                                        let mut path = working_directory.clone();
                                        path.push(name);
                                        println!("READ PATH: {}", path.to_str().unwrap());
                                        let contents;
                                        {
                                            let f = OpenOptions::new()
                                                                .read(true)
                                                                .open(&path)
                                                                .unwrap();
                                            contents = BufReader::new(f).lines()
                                                            .map(|l| l.unwrap_or("".to_string()))
                                                            .collect::<Vec<String>>()
                                        }
                                        contents
                                     })
                                     .collect::<Vec<String>>();
        assert_eq!(contents, vec!["1", "2", "3", "4"]);

        for name in expected_files {
            let mut path = working_directory.clone();
            path.push(name);
            let _ = remove_file(path);
        }
    }
}
