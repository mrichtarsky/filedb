/// Global settings
const DECOMPRESS_ARCHIVES: bool = false;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate serial_test;

use std::{collections::HashMap, collections::HashSet, fs, fs::File, io, path::Path, path::PathBuf, time};
use std::ffi::{OsStr, OsString};

use flate2::Compression;
use flate2::write::ZlibEncoder;
use flate2::read::{GzDecoder, ZlibDecoder};

use fs_extra::dir::CopyOptions;

use separator::Separatable;

use tar::Archive;

use tempdir::TempDir;

use walkdir::WalkDir;

use xz::read::XzDecoder;


type Hash256 = [u8; 32];
const EMPTY_HASH: Hash256 = [0 as u8; 32];
type PathToIndexMap = HashMap<OsString, u32>;
type DirToFilesMap = HashMap<u32, Vec<u32>>;

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, Debug, Clone)]
struct FileDbEntry {
    name: OsString,
    is_dir: bool,
    parent: u32,
    size: u64,
    modified: u64,
    //created: u64, // Not supported on file system
    accessed: u64,
    hash: Hash256,
}

type FileDb = Vec<FileDbEntry>;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{UNIX_EPOCH, Duration};

    use chrono::Local;
    use chrono::prelude::DateTime;
    use fs_extra::dir::copy;

    use super::*;

    const TEST_DATA_DIR: &str = "/home/mrich/projects/filedb/testdata";
    const EXPECTED_DATA_DIR: &str = "/home/mrich/projects/filedb/expected";
    const TEST_WORK_DIR: &str = "/home/mrich/projects/filedb/test_work";
    const WRITE_EXPECTED_RESULTS: bool = false;

    fn check_expected_results(testname: &str, file_db: &FileDb) {
        let mut path_buf = PathBuf::from(EXPECTED_DATA_DIR);
        fs::create_dir_all(&path_buf).unwrap();
        path_buf.push(testname);
        let path = path_buf.as_path();
        if WRITE_EXPECTED_RESULTS {
            save_compressed(path, file_db);
        } else {
            let file_db_expected = load_compressed(path);
            assert!(file_db.len() == file_db_expected.len());
            for (entry, expected_entry) in file_db.iter().zip(file_db_expected) {
                assert!(entry.name == expected_entry.name);
                assert!(entry.is_dir == expected_entry.is_dir);
                assert!(entry.parent == expected_entry.parent);
                assert!(entry.size == expected_entry.size);
                assert!(entry.hash == expected_entry.hash);
            }
        }
    }

    fn get_sizes(file_db: &FileDb) -> Vec<u64> {
        file_db.iter().map(|entry| entry.size).collect::<Vec<u64>>()
    }

    fn _get_time_string(epoch_seconds: u64) -> String {
        let d = UNIX_EPOCH + Duration::from_secs(epoch_seconds);
        let datetime = DateTime::<Local>::from(d);
        datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string()
    }

    fn dump_file_db(file_db: &FileDb)
    {
        println!("{} entries", file_db.len());
        for (i, entry) in file_db.iter().enumerate() {
            let full_path = get_full_path(file_db, i as u32);
            // println!("#{}: {:?}, size: {}, modified: {}, accessed: {}",
            //         i, full_path, entry.size, get_time_string(entry.modified), get_time_string(entry.accessed));
            println!("#{}: {:?}, size: {}", i, full_path, entry.size);
        }
    }

    #[test]
    fn test_propagate_basic() {
        let mut file_db = Vec::new();
        file_db.push(FileDbEntry {
            name: OsString::from("/"),
            is_dir: true,
            parent: std::u32::MAX,
            size: 0,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry {
            name: OsString::from("file.txt"),
            is_dir: false,
            parent: 0,
            size: 10,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        propagate_sizes(&mut file_db);
        assert_eq!(get_sizes(&file_db), vec!(10, 10));
    }

    #[test]
    fn test_propagate_uneven_levels() {
        let mut file_db = Vec::new();
        file_db.push(FileDbEntry {
            name: OsString::from("/test"),
            is_dir: true,
            parent: std::u32::MAX,
            size: 0,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry {
            name: OsString::from("a"),
            is_dir: true,
            parent: 0,
            size: 0,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry {
            name: OsString::from("b"),
            is_dir: true,
            parent: 1,
            size: 0,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry {
            name: OsString::from("c"),
            is_dir: true,
            parent: 2,
            size: 0,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry {
            name: OsString::from("dd"),
            is_dir: false,
            parent: 3,
            size: 10,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry {
            name: OsString::from("b"),
            is_dir: false,
            parent: 0,
            size: 100,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        propagate_sizes(&mut file_db);
        assert_eq!(get_sizes(&file_db), vec!(110, 10, 10, 10, 10, 100));
    }

    #[test]
    fn test_propagate_incremental() {
        let mut file_db = Vec::new();
        file_db.push(FileDbEntry { // 0, /
            name: OsString::from("/"),
            is_dir: true,
            parent: std::u32::MAX,
            size: 0,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry { // 1, /d1
            name: OsString::from("d1"),
            is_dir: true,
            parent: 0,
            size: 0,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry { // 2, /d1/d2
            name: OsString::from("d2"),
            is_dir: true,
            parent: 1,
            size: 0,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry { // 3, /d1/d2/d3
            name: OsString::from("b"),
            is_dir: true,
            parent: 2,
            size: 0,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry { // 4, /d1/f1
            name: OsString::from("f1"),
            is_dir: false,
            parent: 1,
            size: 100,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry { // 5, /d1/d2/f2
            name: OsString::from("dd"),
            is_dir: false,
            parent: 2,
            size: 10,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });

        propagate_sizes(&mut file_db);
        assert_eq!(get_sizes(&file_db), vec!(110, 110, 10, 0, 100, 10));

        file_db.push(FileDbEntry { // 6, /d1/d2/d4
            name: OsString::from("d4"),
            is_dir: true,
            parent: 2,
            size: 0,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        file_db.push(FileDbEntry { // 7, /d1/d2/d4/f3
            name: OsString::from("f3"),
            is_dir: false,
            parent: 6,
            size: 200,
            modified: 1,
            accessed: 1,
            hash: EMPTY_HASH
        });
        propagate_sizes(&mut file_db);
        assert_eq!(get_sizes(&file_db), vec!(310, 310, 210, 0, 100, 10, 200, 200));
    }

    #[test]
    fn test_is_archive()
    {
        assert!(is_archive(Path::new("archive.tar")));
        assert!(is_archive(Path::new("archive.gz")));
        assert!(is_archive(Path::new("archive.xz")));
        assert!(is_archive(Path::new("archive.tgz")));
        assert!(is_archive(Path::new("archive.tar.gz")));
        assert!(is_archive(Path::new("archive.tar.xz")));

        assert!(!is_archive(Path::new("archivetar")));
        assert!(!is_archive(Path::new("archive.zip")));
        assert!(!is_archive(Path::new("archivezip")));
        assert!(!is_archive(Path::new("archivegz")));
        assert!(!is_archive(Path::new("archivexz")));
        assert!(!is_archive(Path::new("archivetgz")));
    }

    #[test]
    fn test_root_dir_components_added()
    {
        // Make sure each subcomponent of the initial root dir is present
        let mut path_buf = PathBuf::from(TEST_DATA_DIR);
        path_buf.push("simple");
        let path = path_buf.as_path();
        let file_db = crawl_initial(&path);
        dump_file_db(&file_db);
        check_expected_results("simple", &file_db);
    }

    fn copy_to_work_dir(test_subdir: &str, work_subdir: &str) -> (PathBuf, PathBuf)
    {
        let mut work_dir = PathBuf::from(TEST_WORK_DIR);
        work_dir.push(work_subdir);
        let _ = fs::remove_dir_all(&work_dir);
        fs::create_dir_all(&work_dir).unwrap();
        let mut test_dir = PathBuf::from(TEST_DATA_DIR);
        test_dir.push(test_subdir);
        copy(&test_dir, &work_dir, &CopyOptions::new()).unwrap();

        (test_dir, work_dir)
    }

    fn do_add_new_dir_within_trailing_root(trailing_slash: bool)
    {
        let (path_simple, mut path) = copy_to_work_dir("simple", "add_new_dir_within_root");
        let mut file_db = crawl_initial(&path);
        dump_file_db(&file_db);
        check_expected_results("add_new_dir_within_root_1", &file_db);

        path.push("simple");
        path.push("a");
        copy(&path_simple, &path, &CopyOptions::new()).unwrap();
        if trailing_slash {
            path.push("simple/");
        } else {
            path.push("simple");
        }
        crawl_add(&mut file_db, &path);
        dump_file_db(&file_db);
        check_expected_results("add_new_dir_within_root_2", &file_db);
    }

    #[test]
    #[serial]
    fn test_add_new_dir_within_root()
    {
        do_add_new_dir_within_trailing_root(false);
    }

    #[test]
    #[serial]
    fn test_add_new_dir_within_root_trailing_slash()
    {
        do_add_new_dir_within_trailing_root(true);
    }

    fn do_add_root_path_components(root_dir: &Path)
    {
        let mut file_db = FileDb::new();
        let mut path_to_index = PathToIndexMap::new();

        println!("Adding dir {:?}", root_dir);
        add_root_path_components(root_dir, &mut file_db, &mut path_to_index);
        dump_file_db(&file_db);

        file_db.clear();
    }

    #[test]
    fn test_add_root_path_components()
    {
        do_add_root_path_components(Path::new("/"));
        do_add_root_path_components(Path::new("/immens"));
        do_add_root_path_components(Path::new("/immens/_backups"));
    }

    #[test]
    fn test_prune_deleted_paths()
    {
        // Pruning the same path should not delete anything
        let mut path_buf = PathBuf::from(TEST_DATA_DIR);
        path_buf.push("simple");
        let path = path_buf.as_path();
        let mut file_db = crawl_initial(&path);
        dump_file_db(&file_db);
        check_expected_results("simple", &file_db);

        prune_deleted_paths(&mut file_db);
        dump_file_db(&file_db);
        check_expected_results("simple", &file_db);
    }

    #[test]
    fn test_update()
    {
        let (_, path) = copy_to_work_dir("simple", "update");
        let file_db = crawl_initial(&path);
        let mut file_db_name = PathBuf::from(TEST_WORK_DIR);
        file_db_name.push("test_update.db");
        save_compressed(&file_db_name, &file_db);
        update(&file_db_name, &path);
    }

    #[test]
    fn test_mv()
    {
        let (_, path) = copy_to_work_dir("simple", "mv");
        let mut file_db_name = PathBuf::from(TEST_WORK_DIR);
        file_db_name.push("test_mv.db");
        {
            let file_db = crawl_initial(&path);
            dump_file_db(&file_db);
            check_expected_results("mv_before", &file_db);
            save_compressed(&file_db_name, &file_db);
        }
        mv(&file_db_name, Path::new("/home/mrich/projects/filedb/test_work/mv/simple/b"), Path::new("/home/mrich/projects/filedb/test_work/mv/simple/a"));
        let file_db_new = load_compressed(&file_db_name);
        dump_file_db(&file_db_new);
        check_expected_results("mv_after", &file_db_new);
    }
}

fn get_secs(time: &time::SystemTime) -> u64
{
    time.duration_since(time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn is_root_index(entry_index: u32) -> bool {
    entry_index == 0
}

fn get_full_path(file_db: &FileDb, entry_index: u32) -> PathBuf
{
    let mut components: Vec<&OsString> = vec![];
    let mut index = entry_index;
    let mut seen_paths = HashSet::<u32>::new();
    loop {
        let entry = &file_db[index as usize];
        components.push(&entry.name);
        if is_root_index(index) {
            break;
        }
        seen_paths.insert(index);
        assert!(!seen_paths.contains(&entry.parent)); // Detect loops
        index = entry.parent;
    }
    let result: PathBuf = components.iter().rev().collect();
    result
}

fn propagate_sizes(file_db: &mut FileDb)
{
    // Reset so incremental works, too.
    for entry in file_db.iter_mut() {
        if entry.is_dir {
            entry.size = 0;
        }
    }

    let mut levels: Vec<u16> = vec![std::u16::MAX; file_db.len()];
    levels[0] = 0;
    let mut max_level = 0;
    for i in 1..file_db.len() {
        let parent_index = file_db[i].parent as usize;
        assert!(levels[parent_index] != std::u16::MAX);
        levels[i] = levels[parent_index] + 1;
        if levels[i] > max_level {
            max_level = levels[i];
        }
    }
    // Propagate sizes from bottom to top
    // Level 0 is the root, don't propagate
    for level_to_propagate in (1..max_level + 1).rev() {
        for (i, level) in levels.iter().enumerate() {
            if *level == level_to_propagate {
                let parent_index = file_db[i].parent as usize;
                file_db[parent_index].size += file_db[i].size;
            }
        }
    }
}

fn propagate_hashes(file_db: &mut FileDb)
{
    // Reset so incremental works, too.
    for entry in file_db.iter_mut() {
        if entry.is_dir {
            entry.hash = EMPTY_HASH;
        }
    }

    let mut levels: Vec<u16> = vec![std::u16::MAX; file_db.len()];
    let mut dir_to_entries = HashMap::<u32, Vec<u32>>::new();
    levels[0] = 0;
    let mut max_level = 0;
    for i in 1..file_db.len() {
        if file_db[i].is_dir { // Files already have hashes. They are kept at std::u16::MAX and thus never processed by the loop below.
            let parent_index = file_db[i].parent as usize;
            assert!(levels[parent_index] != std::u16::MAX);
            levels[i] = levels[parent_index] + 1;
            assert!(levels[i] < std::u16::MAX);
            if levels[i] > max_level {
                max_level = levels[i];
            }
            dir_to_entries.entry(i as u32).or_insert(Vec::<u32>::new());
        }
        let entry = dir_to_entries.entry(file_db[i].parent).or_insert(Vec::<u32>::new());
        (*entry).push(i as u32);
    }

    // Propagate sizes from bottom to top
    // Level 0 is the root, don't propagate
    assert!(max_level < std::u16::MAX - 1);
    for level_to_propagate in (1..max_level + 1).rev() {
        for (entry_index, level) in levels.iter().enumerate() {
            if *level == level_to_propagate {
                // This will only hit dir entries
                let dir_entries = dir_to_entries.get_mut(&(entry_index as u32)).unwrap();
                dir_entries.sort_by_key(|entry| &file_db[*entry as usize].name);
                let mut hasher = blake3::Hasher::new();
                for dir_entry in dir_entries {
                    hasher.update(&file_db[*dir_entry as usize].hash);
                }
                file_db[entry_index].hash = hasher.finalize().into();
                let ent = &file_db[entry_index];
                assert!(ent.hash != EMPTY_HASH, "{:?}", ent.name);
            }
        }
    }
    // All entries except the root
    assert!(file_db.iter().skip(1).filter(|entry| entry.hash == EMPTY_HASH).peekable().peek().is_none());

    // let empty_hashes = file_db.iter().enumerate().filter(|(index, entry)| entry.hash == EMPTY_HASH).collect::<Vec<_>>();
    // for (index, _) in empty_hashes {
    //     let p = get_full_path(&file_db, index as u32);
    //     println!("{:?}", p);
    // }
}

fn save_compressed(filename: &Path, file_db: &FileDb)
{
    println!("Saving db to {:?}", filename);
    let writer = io::BufWriter::new(File::create(filename).unwrap());
    let encoder = ZlibEncoder::new(writer, Compression::fast());
    bincode::serialize_into(encoder, &file_db).unwrap();
    println!("Done");
}

fn load_compressed(filename: &Path) -> FileDb
{
    println!("Loading db from {:?}", filename);
    let reader = io::BufReader::new(File::open(filename).unwrap());
    let decoder = ZlibDecoder::new(reader);
    let file_db = bincode::deserialize_from(decoder).unwrap();
    println!("Done");
    file_db
}

fn get_ext(path: &Path) -> Option<&str>
{
    let ext = path.extension();
    if ext.is_none() {
        return None;
    } else {
        return Some(ext.unwrap().to_str().unwrap());
    }
}

fn is_archive(path: &Path) -> bool
{
    let ext = get_ext(path);
    if ext.is_none() {
        return false;
    }
    let ext_str = ext.unwrap();
    return ext_str == "tar" || ext_str == "xz" || ext_str == "gz" || ext_str == "tgz";
}

fn decompress_zip(path: &Path, tmp_dir: &TempDir)
{
    println!("decompressing {:?}", path);

    let file = File::open(path).unwrap();

    let mut archive = zip::ZipArchive::new(file).unwrap();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).unwrap();
        #[allow(deprecated)]
        let outpath_rel = file.sanitized_name();
        let outpath = PathBuf::from(tmp_dir.path()).join(outpath_rel);

        if (&*file.name()).ends_with('/') {
            println!("zip creating dir {:?}", outpath);
            fs::create_dir_all(&outpath).unwrap();
        } else {
            if let Some(p) = outpath.parent() {
                if !p.exists() {
                    fs::create_dir_all(&p).unwrap();
                }
            }
            let mut outfile = File::create(&outpath).unwrap();
            io::copy(&mut file, &mut outfile).unwrap();
        }

        // Get and Set permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            if let Some(mode) = file.unix_mode() {
                fs::set_permissions(&outpath, fs::Permissions::from_mode(mode)).unwrap();
            }
        }
    }
}

fn decompress_to_tmp_dir(path: &Path) -> Option<TempDir>
{
    println!("Depacking {:?}", path);

    let ext_str = get_ext(path).unwrap();
    let file = File::open(path).unwrap();
    let tmp_dir = TempDir::new("filedb-decomp").unwrap();
    match ext_str {
        "gz" => {
            let mut gz_decoder = GzDecoder::new(file);
            let mut path_buf = PathBuf::from(tmp_dir.path());
            path_buf.push(path.file_stem().unwrap());
            let mut dest_file = File::create(path_buf).unwrap();
            if io::copy(&mut gz_decoder, &mut dest_file).is_err() {
                return None
            }
        }
        "tar" => {
            let mut archive = Archive::new(file);
            if archive.unpack(tmp_dir.path()).is_err() {
                return None
            }
        }
        "tgz" => {
            let gz_decoder = GzDecoder::new(file);
            let mut archive = Archive::new(gz_decoder);
            if archive.unpack(tmp_dir.path()).is_err() {
                return None
            }
        }
        "xz" => {
            let mut xz_decoder = XzDecoder::new(file);
            let mut path_buf = PathBuf::from(tmp_dir.path());
            path_buf.push(path.file_stem().unwrap());
            let mut dest_file = File::create(path_buf).unwrap();
            if io::copy(&mut xz_decoder, &mut dest_file).is_err() {
                return None
            }
        }
        "zip" => decompress_zip(path, &tmp_dir),
        _ => panic!("unreachable"),
    }
    Some(tmp_dir)
}

fn add_files_from_archive(
    path: &Path,
    file_db: &mut FileDb,
    path_to_index: &mut PathToIndexMap,
    dir_to_file_indexes: &DirToFilesMap,
    replace_prefix_to: &Path)
{
    let tmp_dir = decompress_to_tmp_dir(path);
    if tmp_dir.is_some() {
        add_dir_recursive(
            tmp_dir.unwrap().path(),
            file_db,
            path_to_index,
            dir_to_file_indexes,
            replace_prefix_to
        );
    } else {
        eprintln!("Error unpacking archive {:?}", path);
    }
}

fn replace_prefix(
    path: &Path,
    replace_from: &Path,
    replace_to: &Path) -> PathBuf
{
    if replace_to.as_os_str().len() == 0 {
        return path.to_path_buf();
    }
    if replace_to.as_os_str().len() > 0 {
        let path_stripped = path.strip_prefix(replace_from).unwrap();
        if path_stripped.to_str().unwrap().len() == 0 {
            replace_to.to_path_buf()
        } else {
            replace_to.to_path_buf().join(path_stripped)
        }
    } else {
        path.to_path_buf()
    }
}

fn get_hash_for_file(path: &Path) -> io::Result<Hash256>
{
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    io::copy(&mut file, &mut hasher)?;
    Ok(hasher.finalize().into())
}

fn add_file_db_entry(
    file_db: &mut FileDb,
    file_db_entry: FileDbEntry
) -> u32
{
    assert!(file_db.len() < (std::u32::MAX - 1) as usize, "Maximum size exceeded, only {} files/dirs supported", std::u32::MAX);
    file_db.push(file_db_entry);
    (file_db.len() - 1) as u32
}

fn add_root_path_components(
    root_dir: &Path,
    file_db: &mut FileDb,
    path_to_index: &mut PathToIndexMap)
{
    assert!(!DECOMPRESS_ARCHIVES, "Not implemented in this code block, please fix if needed");

    assert!(!path_to_index.contains_key(root_dir.as_os_str()), "Existing path added, not supported");

    // Add all components of the root dir, including "/", as separate entries, since WalkDir won't report them
    let mut root_dir_prefix = root_dir.clone();
    let mut components = Vec::<&OsStr>::new();
    let mut found;
    loop {
        found = path_to_index.contains_key(root_dir_prefix.as_os_str());
        if found || root_dir_prefix.parent().is_none() {
            break;
        } else {
            components.push(root_dir_prefix.file_name().unwrap());
            root_dir_prefix = root_dir_prefix.parent().unwrap();
        }
    }
    let mut parent_index = std::u32::MAX;
    if found {
        parent_index = *path_to_index.get(root_dir_prefix.as_os_str()).unwrap();
    } else {
        components.push(root_dir_prefix.as_os_str());
    }
    let mut root_dir_tmp = PathBuf::new();
    root_dir_tmp.push(root_dir_prefix);
    for dir_name in components.into_iter().rev() {
        root_dir_tmp.push(dir_name);
        let metadata = fs::metadata(&root_dir_tmp).unwrap();
        let file_db_entry = FileDbEntry {
            name: dir_name.to_owned(),
            is_dir: true,
            parent: parent_index,
            size: 0,
            modified: get_secs(&metadata.modified().unwrap()),
            accessed: get_secs(&metadata.accessed().unwrap()),
            hash: EMPTY_HASH,
        };
        parent_index = file_db.len() as u32;
        add_file_db_entry(file_db, file_db_entry);
        path_to_index.insert(root_dir_tmp.as_os_str().to_owned(), parent_index);
    }
}

fn build_dir_to_files_map(
    file_db: &FileDb,
    path_to_index: &PathToIndexMap) -> DirToFilesMap
{
    let mut dir_to_files_map = DirToFilesMap::new();
    for (path, path_index) in path_to_index {
        assert!(PathBuf::from(path) == get_full_path(file_db, *path_index));
        dir_to_files_map.insert(*path_index, Vec::<u32>::new());
    }
    for (index, entry) in file_db.iter().enumerate() {
        if !entry.is_dir {
            let path_entry = dir_to_files_map.get_mut(&entry.parent).unwrap();
            path_entry.push(index as u32);
        }
    }
    dir_to_files_map
}

// Works for files and dirs, returns 0 for dirs
fn get_hash_for_path(path: &Path, is_dir: bool) -> Hash256
{
    let mut hash = EMPTY_HASH;
    if !is_dir {
        let hash_result = get_hash_for_file(path);
        if hash_result.is_err() {
            eprintln!("Error accessing {:?}", path);
        } else {
            hash = hash_result.unwrap();
        }
    }
    hash
}

fn add_dir_recursive(
    root_dir_: &Path,
    file_db: &mut FileDb,
    path_to_index: &mut PathToIndexMap,
    dir_to_file_indexes: &DirToFilesMap,
    replace_prefix_to: &Path)
{
    println!("Adding dir {:?}", root_dir_);
    assert!(root_dir_.is_absolute());

    let is_update = dir_to_file_indexes.len() > 0;

    // Remove trailing slashes
    let mut root_dir_str = String::from(root_dir_.as_os_str().to_str().unwrap());
    while root_dir_str.ends_with('/') {
        root_dir_str.pop();
    }
    let root_dir_buf = PathBuf::from(root_dir_str);
    let root_dir = root_dir_buf.as_path();

    if !is_update {
        add_root_path_components(root_dir, file_db, path_to_index);
    }

'walker:
    for result_dir_entry in WalkDir::new(root_dir)
        .follow_links(false)
        .contents_first(false)
    {
        let dir_entry = result_dir_entry.unwrap();
        let mut path: PathBuf = dir_entry.path().to_path_buf();

        // When dealing with an archive that was unpacked to a temporary directory:
        // Replace the path prefix of the temporary directory with the full path to
        // the archive. Proceed with handling as usual. The archive is thus handled
        // like a directory, with the contents added beneath it.
        path = replace_prefix(&path, root_dir, replace_prefix_to);

        let path_os_str = path.as_os_str();

        if path_to_index.contains_key(path_os_str) {
            continue;
        }

        let metadata = dir_entry.metadata().unwrap();
        let is_dir = metadata.is_dir();

        if DECOMPRESS_ARCHIVES && is_archive(&path) && !is_dir {
            add_files_from_archive(dir_entry.path(), file_db, path_to_index, dir_to_file_indexes, &path);
        } else {
            let parent_path = path.parent().unwrap();
            let parent_index = *path_to_index.get(parent_path.as_os_str()).unwrap();

            let file_name = path.file_name().unwrap().to_os_string();

            if is_update {
                if is_dir {
                    let entry = path_to_index.get(path_os_str);
                    if entry.is_some() {
                        continue 'walker;
                    }
                } else {
                    let dir_entry = path_to_index.get(parent_path.as_os_str());
                    if dir_entry.is_some() {
                        let file_entries_opt = dir_to_file_indexes.get(dir_entry.unwrap());
                        if file_entries_opt.is_some() {
                            for file_index in file_entries_opt.unwrap() {
                                let entry = &file_db[*file_index as usize];
                                if entry.name == file_name {
                                    continue 'walker;
                                }
                            }
                        }
                    }
                }
            }

            let modified_secs = get_secs(&metadata.modified().unwrap());
            let accessed_secs = get_secs(&metadata.accessed().unwrap());
            let hash = get_hash_for_path(dir_entry.path(), is_dir);
            println!("Adding {:?}", &path);

            let file_db_entry = FileDbEntry {
                name: file_name,
                is_dir: is_dir,
                parent: parent_index,
                size: if is_dir { 0 } else { metadata.len() },
                modified: modified_secs, // Note: For archives,
                accessed: accessed_secs, // this is the depack time
                hash: hash,
            };
            add_file_db_entry(file_db, file_db_entry);
            if (file_db.len() % 1000) == 0 {
                println!("{}", file_db.len());
            }

            if is_dir {
                let path_owned = path.as_os_str().to_owned();
                assert!(!path_to_index.contains_key(&path_owned));
                path_to_index.insert(path_owned, (file_db.len() - 1) as u32);
            }
        }
    }
}

fn crawl_initial(root_dir: &Path) -> FileDb
{
    let mut file_db: FileDb = Vec::new();
    let mut path_to_index: PathToIndexMap = HashMap::new();
    let dir_to_file_indexes = DirToFilesMap::new();

    add_dir_recursive(
        root_dir,
        &mut file_db,
        &mut path_to_index,
        &dir_to_file_indexes,
        Path::new("")
    );
    file_db
}

fn build_path_to_index_map(file_db: &FileDb) -> PathToIndexMap
{
    let mut path_to_index = PathToIndexMap::new();

    for (i, entry) in file_db.iter().enumerate() {
        if entry.is_dir {
            let path = get_full_path(&file_db, i as u32);
            path_to_index.insert(path.as_os_str().to_owned(), i as u32);
        }
    }
    path_to_index
}

fn crawl_add(file_db: &mut FileDb, root_dir: &Path)
{
    let mut path_to_index = build_path_to_index_map(file_db);

    let dir_to_file_indexes = DirToFilesMap::new();

    add_dir_recursive(
        Path::new(root_dir),
        file_db,
        &mut path_to_index,
        &dir_to_file_indexes,
        Path::new("")
    );
}

pub fn add(file_db_name: &Path, root_dir: &Path)
{
    let mut file_db;
    if fs::metadata(file_db_name).map_or(false, |metadata| metadata.is_file()) {
        file_db = load_compressed(file_db_name);
        crawl_add(&mut file_db, root_dir);
    } else {
        file_db = crawl_initial(root_dir);
    }

    propagate_sizes(&mut file_db);

    save_compressed(file_db_name, &file_db);
}

fn prune_deleted_paths(file_db: &mut FileDb)
{
    println!("Pruning deleted paths");
    let mut new_file_db = FileDb::new();
    let mut deleted_entries = 0;
    let mut path_to_index = PathToIndexMap::new();
    for entry_index in 0..file_db.len() {
        let path = get_full_path(file_db, entry_index as u32);
        let metadata_res = fs::symlink_metadata(&path);
        if metadata_res.is_ok() {
            let metadata = metadata_res.unwrap();
            let entry = &file_db[entry_index as usize];
            if metadata.is_dir() != entry.is_dir
                || (!metadata.is_dir() && metadata.len() != entry.size)
                || (!metadata.is_dir() && get_secs(&metadata.modified().unwrap()) != entry.modified)
            {
                assert!(!entry.is_dir, "Not implemented (need to remove all referencing paths)");
                deleted_entries += 1;
            } else {
                let mut entry_copy = entry.clone();
                if !is_root_index(entry_index as u32) {
                    let parent_path = path.parent().unwrap();
                    let parent_index = path_to_index.get(parent_path.as_os_str()).unwrap();
                    entry_copy.parent = *parent_index;
                }
                add_file_db_entry(&mut new_file_db, entry_copy);
                if entry.is_dir {
                    path_to_index.insert(path.as_os_str().to_owned(), (new_file_db.len() - 1) as u32);
                }
            }
        } else {
            deleted_entries += 1;
        }
    }
    println!("Pruned {} paths, old: {}, new: {}", deleted_entries, file_db.len(), new_file_db.len());
    *file_db = new_file_db;
    file_db.shrink_to_fit();
}

// root_dir must be the original root dir used for the file_db,
// otherwise behavior is undefined (may still work but untested)
pub fn update(file_db_name: &Path, root_dir: &Path)
{
    let mut file_db = load_compressed(file_db_name);
    prune_deleted_paths(&mut file_db);

    let mut path_to_index = build_path_to_index_map(&file_db);
    let dir_to_files = build_dir_to_files_map(&file_db, &path_to_index);

    add_dir_recursive(
        root_dir,
        &mut file_db,
        &mut path_to_index,
        &dir_to_files,
        Path::new("")
    );

    save_compressed(file_db_name, &file_db);
}

pub fn mv(file_db_name: &Path, from_dir: &Path, to_dir: &Path)
{
    let mut file_db = load_compressed(file_db_name);
    let from_metadata = fs::metadata(from_dir);
    let to_metadata = fs::metadata(to_dir);
    if from_metadata.is_err() {
        panic!("source directory does not exist: {:?}", from_dir);
    }
    if to_metadata.is_err() {
        panic!("target directory does not exist: {:?}", to_dir);
    }
    if from_metadata.unwrap().is_dir() && to_metadata.unwrap().is_dir() {
        let mut target_path = PathBuf::from(to_dir);
        target_path.push(from_dir.file_name().unwrap());
        let target_metadata = fs::metadata(&target_path);
        if !target_metadata.is_err() {
            panic!("Target dir {:?} exists", target_path);
        }
        let mut to_index = std::usize::MAX;
        let mut from_index = std::usize::MAX;
        for entry_index in 0..file_db.len() {
            if !file_db[entry_index].is_dir {
                continue;
            }
            let full_path = get_full_path(&file_db, entry_index as u32);
            if full_path == to_dir {
                to_index = entry_index;
            }
            if full_path == from_dir {
                from_index = entry_index;
            }
        }
        assert!(to_index != std::usize::MAX && from_index != std::usize::MAX);
        file_db[from_index].parent = to_index as u32;
        println!("Moving data");
        fs_extra::move_items(&[from_dir], to_dir, &CopyOptions::new()).unwrap();
        propagate_sizes(&mut file_db);
        save_compressed(file_db_name, &file_db);
    }
}

fn dump_helper(file_db: &FileDb, full: bool)
{
    for index in 0..file_db.len() {
        let entry = &file_db[index];
        let path = get_full_path(&file_db, index as u32);
        let out_string = format!("{:?}", path);
        let stripped_string = out_string.strip_prefix("\"").unwrap().strip_suffix("\"").unwrap();
        if full {
            println!("{} {} {:?}", stripped_string, entry.size, entry.hash);
        } else {
            println!("{}", stripped_string);
        }
    }
}

pub fn dump(file_db_name: &Path)
{
    let file_db = load_compressed(file_db_name);
    dump_helper(&file_db, false);
}

pub fn dump_full(file_db_name: &Path)
{
    let file_db = load_compressed(file_db_name);
    dump_helper(&file_db, true);
}

pub fn stats(file_db_name: &Path, prefix: Option<&Path>)
{
    let file_db = load_compressed(file_db_name);

    let mut num_files = 0;
    let mut num_dirs = 0;
    let mut size = 0;
    for (index, entry) in file_db.iter().enumerate() {
        if prefix.is_some() {
            let full_path = get_full_path(&file_db, index as u32);
            if !full_path.starts_with(prefix.unwrap()) {
                continue;
            }
        }
        if entry.is_dir {
            num_dirs += 1;
        } else {
            num_files += 1;
            size += entry.size;
        }
    }
    let (largest_entry_name, largest_entry_size) = file_db
        .iter()
        .map(|entry| (&entry.name, entry.size))
        .max_by_key(|elem| elem.1)
        .unwrap();
    println!(
        "Entries: {}, files: {}, dirs: {}, size: {}",
        file_db.len().separated_string(),
        num_files.separated_string(),
        num_dirs.separated_string(),
        size.separated_string()
    );
    println!(
        "Largest entry: {}, size: {}",
        largest_entry_name.to_str().unwrap(),
        largest_entry_size.separated_string()
    );
}

// Check whether all files in backup_dir are elsewhere, and list those that aren't
// Comparison is done by 256bit hash and size, not by name or content
// Ignores empty files (also does not remove them)
pub fn all_files_elsewhere(file_db_name: &Path, backup_dir: &Path, remove_dupes: bool)
{
    // Add all files outside of backup_dir to lookup structure
    let file_db = load_compressed(file_db_name);
    let mut hash_to_index: HashMap<Hash256, Vec<u32>> = HashMap::new();
    for (i, entry) in file_db.iter().enumerate() {
        if entry.is_dir || entry.size == 0 {
            continue;
        }
        let entry_path = get_full_path(&file_db, i as u32);
        if !entry_path.starts_with(backup_dir) {
            let map_entry = hash_to_index.entry(entry.hash).or_insert(Vec::<u32>::new());
            (*map_entry).push(i as u32);
        }
    }

    let mut num_dupes = 0;
    let mut num_files_missing = 0;
    let mut num_dupes_sum = 0;
    let mut num_dupe_entries = 0;
    let mut min_num_dupes = std::usize::MAX;
    let mut max_num_dupes = 0;
    let mut entry_and_dupes = vec!();
    let mut num_duped_bytes = 0;
    let mut num_missing_bytes = 0;
    let mut num_dirs = 0;
    let mut num_empty_files = 0;
    // Iterate all files in backup_dir and check if they are present in lookup structure
    for (i, entry) in file_db.iter().enumerate() {
        let entry_path = get_full_path(&file_db, i as u32);
        if !entry_path.starts_with(backup_dir) {
            continue;
        }
        if entry.is_dir {
            num_dirs += 1;
            continue;
        }
        if entry.size == 0 {
            num_empty_files += 1;
            continue;
        }
        let hash = entry.hash;
        let value = hash_to_index.get(&hash);
        if value.is_none() {
            println!("File missing: {:?}", entry_path);
            num_files_missing += 1;
            num_missing_bytes += entry.size;
        } else {
            let dupe_list = value.unwrap();
            let mut found = false;
            for dupe in dupe_list {
                let dupe_entry = &file_db[*dupe as usize];
                if dupe_entry.size == entry.size /*&& dupe_entry.name == entry.name*/ {
                    found = true;
                    break;
                }
            }
            if !found {
                println!("File missing: {:?}", entry_path);
                num_files_missing += 1;
                num_missing_bytes += entry.size;
            } else {
                let num_entry_dupes = dupe_list.len();
                num_dupes_sum += num_entry_dupes;
                num_dupe_entries += 1;
                min_num_dupes = std::cmp::min(min_num_dupes, num_dupes);
                max_num_dupes = std::cmp::max(max_num_dupes, num_dupes);
                entry_and_dupes.push((i as u32, dupe_list));
                num_dupes += 1;
                num_duped_bytes += entry.size;
                if fs::metadata(&entry_path).is_ok() {
                    if remove_dupes {
                        println!("Removing {:?}", entry_path);
                        let res = fs::remove_file(&entry_path);
                        if res.is_err() {
                            println!("Error removing {:?}", entry_path);
                        }
                        let mut parent = entry_path.parent().unwrap();
                        while fs::remove_dir(parent).is_ok() { // Will only remove empty dirs
                            println!("Removed parent dir {:?}", parent);
                            parent = parent.parent().unwrap();
                        }
                    } else {
                        println!("Would remove {:?}", entry_path);
                    }
                }
            }
        }
    }

    println!("Num dupes: {}", num_dupes);
    println!("Files missing: {}", num_files_missing);
    println!("Dirs: {}", num_dirs);
    println!("Empty files: {}", num_empty_files);
    println!("");
    println!("Min num dupes: {}", min_num_dupes);
    println!("Max num dupes: {}", max_num_dupes);
    println!("Avg num dupes: {}", num_dupes_sum / num_dupe_entries);
    println!("Num duped bytes: {}", num_duped_bytes);
    println!("Num missing bytes: {}", num_missing_bytes);
}

pub fn dedup(file_db_name: &Path)
{
    let mut file_db = load_compressed(file_db_name);
    propagate_hashes(&mut file_db);

    let mut hash_and_size_to_indices = HashMap::<(Hash256, u64), Vec<u32>>::new();
    for (index, entry) in file_db.iter().enumerate() {
        let indices = hash_and_size_to_indices.entry((entry.hash, entry.size)).or_insert(Vec::<u32>::new());
        indices.push(index as u32);
    }
    let mut num_duped_bytes = 0;
    let mut max_dupe_count = 0;
    let mut entries = hash_and_size_to_indices.iter().collect::<Vec<_>>();
    entries.sort_by_key(|((_, size), dupes)| size * dupes.len() as u64);
    for (key, indices) in entries.into_iter().rev() {
        let (_, size) = key;
        let dupe_count = indices.len() - 1;
        if  dupe_count != 0 {
            if dupe_count > max_dupe_count {
                max_dupe_count = dupe_count;
            }
            let duped_bytes = dupe_count as u64 * size;
            println!("Duplicated data of size: {} dupes: {} duped GB: {}", size.separated_string(), dupe_count, duped_bytes / 1024 / 1024 / 1024);
            println!("  Dupe locations:");
            for index in indices {
                let path = get_full_path(&file_db, *index);
                println!("    {:?}", path);
            }
            num_duped_bytes += duped_bytes;
        }
    }
    println!("Total duped bytes: {}", num_duped_bytes.separated_string());
    println!("Max dupe count: {}", max_dupe_count);
}
