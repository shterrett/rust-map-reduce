use std::path::PathBuf;
use std::io::BufReader;
use std::fs::{ File, read_dir };
use std::str::FromStr;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use chan;
use chan::{ Sender, Receiver };
use worker::{ Job, JobResult, Worker };

pub struct Master {
    input_files: Vec<PathBuf>,
    working_directory: PathBuf,
    map: Arc<Fn(BufReader<File>) -> Vec<String> + Send + Sync>,
    reduce: Arc<Fn(Vec<BufReader<File>>) -> String + Send + Sync>,
    job_queue: Sender<Job>,
    results_queue: Receiver<JobResult>,
    worker_job_queue: Receiver<Job>,
    worker_results_queue: Sender<JobResult>
}

impl Master {
    pub fn new(working_directory: PathBuf,
           input_files: Vec<PathBuf>,
           map: Arc<Fn(BufReader<File>) -> Vec<String> + Send + Sync>,
           reduce: Arc<Fn(Vec<BufReader<File>>) -> String + Send + Sync>
           ) -> Self
    {
        let (work_send, work_recv) = chan::async();
        let (result_send, result_recv) = chan::async();

        Master {
            input_files: input_files,
            working_directory: working_directory,
            map: map,
            reduce: reduce,
            job_queue: work_send,
            results_queue: result_recv,
            worker_job_queue: work_recv,
            worker_results_queue: result_send
        }
    }

    fn do_map(&self) -> i32 {
        for (index, input) in self.input_files.iter().enumerate() {
            self.job_queue.send(Job::Map(((index + 1) as i32, input.clone())));
        }

        self.input_files.iter().len() as i32
    }

    fn do_reduce(&self) -> i32 {
        if let Ok(entries) = read_dir(self.working_directory.clone()) {
            let groups = entries.filter_map(|entry| entry.ok())
                                .fold(HashMap::new(), |mut grouped, entry| {
                                    let _ = entry.file_name()
                                                 .into_string()
                                                 .and_then(|filename| {
                                                     filename.split(".")
                                                             .nth(3)
                                                             .and_then(|i| {
                                                                  i32::from_str(i).ok()
                                                             }).ok_or(entry.file_name())
                                                 })
                                                 .map(|key| {
                                                     let mut files = grouped.entry(key).or_insert(vec![]);
                                                     files.push(entry.path())
                                                 });
                                    grouped
                                });
            let n_reduce_jobs = groups.iter().len();
            for (index, group) in groups {
                self.job_queue.send(Job::Reduce((index, group)));
            }
            n_reduce_jobs as i32
        } else {
            0
        }
    }

    pub fn run(&self, n_workers: i32) -> Vec<PathBuf> {
        self.spawn_workers(n_workers);

        let n_map = self.do_map();
        self.wait_for_completion(n_map);
        let n_reduce = self.do_reduce();
        self.wait_for_completion(n_reduce);

        self.aggregate_result_files()
    }

    fn spawn_workers(&self, n_workers: i32) {
        for _ in 0..n_workers {
            let working_directory = self.working_directory.clone();
            let map = self.map.clone();
            let reduce = self.reduce.clone();
            let job_queue = self.worker_job_queue.clone();
            let results_queue = self.worker_results_queue.clone();

            thread::spawn(move || {
                let worker = Worker {
                    working_directory: working_directory,
                    map: map,
                    reduce: reduce,
                    job_queue: job_queue,
                    results_queue: results_queue
                };
                worker.run()
            });
        }
    }

    fn wait_for_completion(&self, n_jobs: i32) {
        let mut n_complete = 0;
        while n_complete < n_jobs {
            self.results_queue.recv()
                              .map(|_| {
                                  n_complete += 1
                              });
        }
    }

    fn aggregate_result_files(&self) -> Vec<PathBuf> {
        read_dir(self.working_directory.clone())
            .map(|entries| {
                entries.filter_map(|entry| entry.ok())
                       .filter_map(|entry| {
                           entry.file_name()
                                .into_string()
                                .ok()
                                .and_then(|name| {
                                    match name.split(".").last() {
                                        Some("result") => Some(entry),
                                        _ => None
                                    }
                                })

                       })
                       .map(|entry| entry.path())
                       .collect::<Vec<PathBuf>>()
            })
            .unwrap_or(vec![])
    }
}

#[cfg(test)]
mod test {
    use std::fs::OpenOptions;
    use std::io::{ Write, BufRead, BufReader };
    use std::path::PathBuf;
    use std::fs::{ File, remove_file };
    use std::thread;
    use std::sync::Arc;
    use super::Master;
    use worker::{ Job, JobResult };

    fn map_fn(input: BufReader<File>) -> Vec<String> {
        vec!["1", "2", "3", "4"].iter().map(|s| s.to_string()).collect()
    }
    fn reduce_fn(input: Vec<BufReader<File>>) -> String {
        "1234".to_string()
    }

    #[test]
    fn master_enqueues_map_jobs() {
        let working_directory = PathBuf::from("./test-data/master_enqueues_map_jobs");
        let input_files = vec!["input_1", "input_2", "input_3", "input_4"].into_iter()
            .map(|filename| {
                let mut path = working_directory.clone();
                path.push(filename);
                path
            })
            .collect::<Vec<PathBuf>>();
        let master = Master::new(working_directory.clone(),
                                 input_files.clone(),
                                 Arc::new(map_fn),
                                 Arc::new(reduce_fn)
                                );

        let job_recv = master.worker_job_queue.clone();
        let map_jobs = thread::spawn(move || {
            job_recv.iter().collect::<Vec<Job>>()
        });

        let n_map_jobs = master.do_map();
        drop(master);

        let expected_jobs = input_files.iter()
                                       .enumerate()
                                       .map(|(i, f)| Job::Map(((i + 1) as i32, f.clone())))
                                       .collect::<Vec<Job>>();
        assert_eq!(n_map_jobs, 4);
        assert_eq!(map_jobs.join().unwrap(), expected_jobs);
    }

    #[test]
    fn master_enqueues_reduce_jobs() {
        let working_directory = PathBuf::from("./test-data/master_enqueues_reduce_jobs");
        let input_files = vec!["input_1", "input_2", "input_3", "input_4"].into_iter()
            .map(|filename| {
                let mut path = working_directory.clone();
                path.push(filename);
                path
            })
            .collect::<Vec<PathBuf>>();
        let master = Master::new(working_directory.clone(),
                                 input_files,
                                 Arc::new(map_fn),
                                 Arc::new(reduce_fn)
                                );

        let job_recv = master.worker_job_queue.clone();
        let reduce_jobs = thread::spawn(move || {
            job_recv.iter()
                    .collect::<Vec<Job>>()
                    .sort_by(|fst, snd| {
                        match (fst, snd) {
                            (&Job::Reduce((i, _)), &Job::Reduce((j, _))) => {
                                i.cmp(&j)
                            }
                            _ => {
                                1.cmp(&1)
                            }
                        }
                    })
        });

        let n_reduce_jobs = master.do_reduce();
        drop(master);

        let expected_jobs = (1..(4 + 1)).map(|reduce_id| {
            (1..(4 + 1)).map(|map_id| {
                let mut path = working_directory.clone();
                path.push(format!("map.{}.reduce.{}", map_id, reduce_id));
                path
            }).collect::<Vec<PathBuf>>()
        }).enumerate()
          .map(|(i, f)| Job::Reduce(((i + 1) as i32, f.clone())))
          .collect::<Vec<Job>>()
          .sort_by(|fst, snd| {
              match (fst, snd) {
                  (&Job::Reduce((i, _)), &Job::Reduce((j, _))) => {
                      i.cmp(&j)
                  }
                  _ => {
                      1.cmp(&1)
                  }
              }
          });

        assert_eq!(n_reduce_jobs, 4);
        assert_eq!(reduce_jobs.join().unwrap(), expected_jobs);
    }

    #[test]
    fn run_map_reduce() {
        let working_directory = PathBuf::from("./test-data/master_runs_map_reduce");
        let input_files = vec!["input_1", "input_2", "input_3", "input_4"].into_iter()
            .map(|filename| {
                let mut path = working_directory.clone();
                path.push(filename);
                path
            })
            .collect::<Vec<PathBuf>>();
        let master = Master::new(working_directory.clone(),
                                 input_files.clone(),
                                 Arc::new(map_fn),
                                 Arc::new(reduce_fn)
                                );

        let result_files = master.run(2);

        let expected_files = vec!["reduce.1.result",
                                  "reduce.2.result",
                                  "reduce.3.result",
                                  "reduce.4.result"
        ].into_iter()
         .map(|filename| {
             let mut path = working_directory.clone();
             path.push(filename);
             path
         })
         .collect::<Vec<PathBuf>>();
        assert_eq!(result_files, expected_files);
        let expected_result = vec!["1234".to_string()];

        for path in expected_files {
            let f = OpenOptions::new()
                                .read(true)
                                .open(&path)
                                .unwrap();
            let contents = BufReader::new(f).lines()
                                     .map(|l| l.unwrap_or("".to_string()))
                                     .collect::<Vec<String>>();
            assert_eq!(contents, expected_result);
        }

        for i in 1..(4 + 1) {
            for j in 1..(4 + 1) {
                let mut map_file = working_directory.clone();
                map_file.push(format!("map.{}.reduce.{}", i, j));
                let _ = remove_file(map_file);
            }
            let mut result_file = working_directory.clone();
            result_file.push(format!("reduce.{}.result", i));
            let _ = remove_file(result_file);
        }
    }
}
