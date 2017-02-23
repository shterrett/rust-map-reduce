use std::path::PathBuf;
use std::io::BufReader;
use std::fs::File ;
use worker::{ Job, JobResult };
use chan;
use chan::{ Sender, Receiver };

struct Master {
    input_files: Vec<PathBuf>,
    n_map: i32,
    n_reduce: i32,
    working_directory: PathBuf,
    map: Box<Fn(BufReader<File>) -> Vec<String> + Send>,
    reduce: Box<Fn(Vec<BufReader<File>>) -> String + Send>,
    job_queue: Sender<Job>,
    results_queue: Receiver<JobResult>,
    worker_job_queue: Receiver<Job>,
    worker_results_queue: Sender<JobResult>
}

impl Master {
    fn new(working_directory: PathBuf,
           input_files: Vec<PathBuf>,
           n_reduce: i32,
           map: Box<Fn(BufReader<File>) -> Vec<String> + Send>,
           reduce: Box<Fn(Vec<BufReader<File>>) -> String + Send>
           ) -> Self
    {
        let (work_send, work_recv) = chan::async();
        let (result_send, result_recv) = chan::async();
        let n_map = input_files.iter().len() as i32;

        Master {
            input_files: input_files,
            n_map: n_map,
            n_reduce: n_reduce,
            working_directory: working_directory,
            map: map,
            reduce: reduce,
            job_queue: work_send,
            results_queue: result_recv,
            worker_job_queue: work_recv,
            worker_results_queue: result_send
        }
    }

    fn do_map(&self) {
        for (index, input) in self.input_files.iter().enumerate() {
            self.job_queue.send(Job::Map(((index + 1) as i32, input.clone())));
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::BufReader;
    use std::path::PathBuf;
    use std::fs::File ;
    use std::thread;
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
                                 4,
                                 Box::new(map_fn),
                                 Box::new(reduce_fn)
                                );

        let job_recv = master.worker_job_queue.clone();
        let map_jobs = thread::spawn(move || {
            job_recv.iter().collect::<Vec<Job>>()
        });

        master.do_map();
        drop(master);

        let expected_jobs = input_files.iter()
                                       .enumerate()
                                       .map(|(i, f)| Job::Map(((i + 1) as i32, f.clone())))
                                       .collect::<Vec<Job>>();
        assert_eq!(map_jobs.join().unwrap(), expected_jobs);
    }
}
