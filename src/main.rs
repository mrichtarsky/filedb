use std::{env, path::Path, process};

use filedb;

fn print_usage_and_exit_with_error()
{
    println!("Usage: filedb path_to_filedb [add [path1 [path2]...]|update|all_files_elsewhere path|all_files_elsewhere_remove_dupes path|dedup|dump|dump_full|stats]");
    process::exit(1);
}

fn main()
{
    let args = env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        print_usage_and_exit_with_error();
    }
    let db_file_name = &args[1];
    let command = &args[2];
    match command.as_str() {
        "add" => {
            for root_path in args.iter().skip(3) {
                filedb::add(Path::new(db_file_name), Path::new(root_path));
            }
        },
        "update" => {
            if args.len() != 4 {
                print_usage_and_exit_with_error();
            }
            let root_dir = Path::new(&args[3]);
            filedb::update(Path::new(db_file_name), root_dir);
        },
        "mv" => {
            if args.len() != 5 {
                print_usage_and_exit_with_error();
            }
            let from_dir = Path::new(&args[3]);
            let to_dir = Path::new(&args[4]);
            filedb::mv(Path::new(db_file_name), from_dir, to_dir);
        },
        "all_files_elsewhere" => {
            if args.len() != 4 {
                print_usage_and_exit_with_error();
            }
            let backup_dir = Path::new(&args[3]);
            filedb::all_files_elsewhere(Path::new(db_file_name), backup_dir, false);
        },
        "all_files_elsewhere_remove_dupes" => {
            if args.len() != 4 {
                print_usage_and_exit_with_error();
            }
            let backup_dir = Path::new(&args[3]);
            filedb::all_files_elsewhere(Path::new(db_file_name), backup_dir, true);
        },
        "dedup" => {
            filedb::dedup(Path::new(db_file_name));
        },
        "dump" => filedb::dump(Path::new(db_file_name)),
        "dump_full" => filedb::dump_full(Path::new(db_file_name)),
        "stats" => {
            if args.len() == 3 {
                filedb::stats(Path::new(db_file_name), None);
            } else {
                for root_path in args.iter().skip(3) {
                    filedb::stats(Path::new(db_file_name), Some(Path::new(root_path)));
                }
            }
        },
        _ => print_usage_and_exit_with_error(),
    }
}
