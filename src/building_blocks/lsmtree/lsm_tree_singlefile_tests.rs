use anyhow::{Result, Context};
use std::fs::{remove_dir_all, create_dir};
use std::ops::Range;
use std::path::Path;
use crate::building_blocks::{
    sstable::SF,
    SSTableBuilderSingleFile as SSTableBuilder,
    Entry
};
use super::LSMTree;

macro_rules! redo_dirs {
    ($expr:expr) => {
        let test_path = $expr;
        let exists = Path::new(test_path).is_dir();
        if exists {
            remove_dir_all(test_path).expect("removing old data");
        }
        create_dir(test_path) .context("creating the test directory")
            .expect("idk");
    };
}

macro_rules! are_tombstones {
    ($lsm:expr, $keys:expr, true) => {
        for key in $keys {
            let out = $lsm.get(Vec::from(key));
            assert!(out.is_some());

            let entry = out.unwrap();
            assert_eq!(Vec::from(key), entry.key);
            assert!(entry.value.is_none());
        }
    };

    ($lsm:expr, $keys:expr, false) => {
        for key in $keys {
            let out = $lsm.get(Vec::from(key));
            assert!(out.is_some());

            let entry = out.unwrap();
            assert_eq!(Vec::from(key), entry.key);
            assert!(entry.value.is_some());
        }
    };
}

macro_rules! keys_exist {
    ($lsm:expr, $keys:expr, true) => {
        for key in $keys {
            let out = $lsm.get(Vec::from(key));
            assert!(out.is_some());
        }
    };

    ($lsm:expr, $keys:expr, false) => {
        for key in $keys {
            let out = $lsm.get(Vec::from(key));
            assert_eq!(out, None);
        }
    };
}

fn insert_range(
    range: &mut Range<i32>,
    dir: &str,
    lsm: &mut LSMTree<SF>,
    tombstone: bool,
    auto_merge: bool,
    base: &str,
) -> Result<(), ()> {
    // must sort entries
    let mut entries: Vec<String> = range.map(|i| i.to_string()).collect();

    entries.sort();

    let mut path;
    if base.is_empty() {
        if tombstone {
            path = String::from("test-tombs-0-0");
        } else {
            path = String::from("test-sstable-0-0");
        }
    } else {
        path = String::from(format!("test-{base}-0-0"));
    }

    let mut builder = SSTableBuilder::new(dir, &path, 100, 0.1, 10)
        .expect("creating a sstable");

    println!("created builder: {dir}/{path}");
    for (idx, key) in entries.iter().enumerate() {
        let val;
        let timestmp;

        if tombstone {
            val = None;
            timestmp = ((idx + 1) * 1000) as u128;
        } else {
            val = Some(key.to_string().into_bytes());
            timestmp = idx as u128;
        }

        let entry = Entry {
            timestamp: timestmp,
            key: key.to_string().into_bytes(),
            value: val,
        };

        builder
            .insert(entry)
            .context("inserting entry into the sstable")
            .unwrap();

        if idx < 100 {
            continue;
        }

        // finish previous and start new
        if idx % 100 == 0 {
            builder
                .finish_data()
                .context(format!("finishing sstable {dir}/{path}"))
                .unwrap();
            println!("finished builder: {dir}/{path}");

            if auto_merge {
                lsm.insert(&path)
                    .context(format!("inserting sstable {dir}/{path}"))
                    .unwrap();
            } else {
                lsm.append_table(&path)
                    .context(format!("inserting sstable {dir}/{path}"))
                    .unwrap();
            }
            println!("inserted sstable: {dir}/{path}");

            if tombstone {
                path = format!("test-tombs-0-{}", idx / 100);
            } else {
                path = format!("test-sstable-0-{}", idx / 100);
            }

            builder = SSTableBuilder::new(dir, &path, 100, 0.1, 10)
                .context(format!("opening sstable {dir}/{path}"))
                .unwrap();
            println!("created builder: {dir}/{path}");
        }
    }

    builder.finish_data().expect("finishing big sstable");
    println!("finished builder: {dir}/{path}");

    if auto_merge {
        lsm.insert(&path)
            .context(format!("inserting sstable {dir}/{path}"))
            .unwrap();
    } else {
        lsm.append_table(&path)
            .context(format!("inserting sstable {dir}/{path}"))
            .unwrap();
    }
    println!("inserted {path}");

    Ok(())
}


#[test]
fn lsm_insert_single() -> Result<(), ()> {
    let test_path = "test-data/lsm-insert-single";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::<SF>::new(0.1, 10, String::from(test_path), 3, 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, false, "")
}

#[test]
fn lsm_read_single() -> Result<(), ()> {
    let test_path = "./test-data/lsm-read-single";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::<SF>::new(0.1, 10, String::from(test_path), 3, 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, false, "").unwrap();

    let keys: Vec<&str> = vec![
        "456", "789", "234", "567", "890", "901", "345", "678", "123", "432", "765", "210", "543",
        "876", "109", "987", "654", "321", "345", "678", "901", "234", "567", "890", "123", "456",
        "789", "432", "765", "210", "543", "876", "109", "987", "654", "321", "345", "678", "901",
        "234", "567", "890", "123",
    ];
    keys_exist!(lsm, keys, true);

    let keys: Vec<&str> = vec![
        "0", "1", "4", "7", "3", "9", "54", "67", "12", "43", "76", "21", "54", "32", "76", "29",
        "87", "54", "21", "45", "78", "71", "34", "67", "90", "23", "56", "89", "32", "65", "10",
        "43", "76", "99", "87", "54", "21", "45", "78", "10", "34", "67", "90", "23",
    ];
    keys_exist!(lsm, keys, true);

    let keys: Vec<&str> = vec![
        "1347", "2659", "8531", "4752", "9812", "5763", "3498", "6781", "9123", "1245", "6537",
        "3091", "7845", "2893", "6534", "1829", "9876", "5432", "1234", "9087", "7654", "2345",
        "8765", "5432", "2134", "7865", "1249", "4301", "8976", "2098", "6234", "4567", "9564",
        "7532", "9987", "5703", "8765", "9284", "6092", "4531", "8635", "4398", "7612", "2089",
        "6152", "3546", "8790", "9843", "5261", "4215", "6714", "3156", "1065", "7834",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "-347", "-659", "-531", "-752", "-812", "-763", "-498", "-781", "-123", "-245", "-537",
        "-091", "-845", "-893", "-534", "-829", "-876", "-432", "-234", "-087", "-654", "-345",
        "-765", "-432", "-134", "-865", "-249", "-301", "-976", "-098", "-234", "-567", "-564",
        "-532", "-987", "-703", "-765", "-284", "-092", "-531", "-635", "-398", "-612", "-089",
        "-152", "-546", "-790", "-843", "-261", "-215", "-714", "-156", "-065", "-834",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "0047", "0059", "0031", "0052", "0012", "0063", "0098", "0081", "0023", "0045", "0037",
        "0091", "0045", "0093", "0034", "0029", "0076", "0032", "0034", "0087", "0054", "0045",
        "0065", "0032", "0034", "0065", "0049", "0001", "0076", "0098", "0034", "0067", "0064",
        "0032", "0087", "0003", "0065", "0084", "0092", "0031", "0035", "0098", "0012", "0089",
        "0052", "0046", "0090", "0043", "0061", "0015", "0014", "0056", "0065", "0034",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "0000", "1111", "2222", "3333", "4444", "5555", "6666", "7777", "8888", "9999", "1234",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "qX2t", "pY9s", "mZ7r", "kA3v", "nB6w", "eC8x", "fD1y", "gE5z", "hF4u", "iG0t", "jH6q",
        "rI3w", "tJ8e", "yK1r", "uL4t", "wM7y", "oN9u", "zO2i", "xP5o", "vQ0p", "aR7s", "bS3d",
        "cT6f", "dU1g", "eV4h", "fW8j", "gX5k", "hY2l", "iZ9m", "jA6n", "kB0p", "lC7q", "mD3r",
        "nE5s", "oF1t", "pG4u", "qH7v", "rI2w", "sJ9x", "tK6y",
    ];
    keys_exist!(lsm, keys, false);

    Ok(())
}

#[test]
fn lsm_merge_simple_single() {
    let test_path = "./test-data/lsm-merge-simple-single";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::<SF>::new(0.1, 10, String::from(test_path), 3, 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, false, "").unwrap();

    lsm.merge(0, test_path).unwrap();

    let keys: Vec<&str> = vec![
        "456", "789", "234", "567", "890", "901", "345", "678", "123", "432", "765", "210", "543",
        "876", "109", "987", "654", "321", "345", "678", "901", "234", "567", "890", "123", "456",
        "789", "432", "765", "210", "543", "876", "109", "987", "654", "321", "345", "678", "901",
        "234", "567", "890", "123",
    ];
    keys_exist!(lsm, keys, true);

    let keys: Vec<&str> = vec![
        "0", "1", "4", "7", "3", "9", "54", "67", "12", "43", "76", "21", "54", "32", "76", "29",
        "87", "54", "21", "45", "78", "71", "34", "67", "90", "23", "56", "89", "32", "65", "10",
        "43", "76", "99", "87", "54", "21", "45", "78", "10", "34", "67", "90", "23",
    ];
    keys_exist!(lsm, keys, true);

    let keys: Vec<&str> = vec![
        "1347", "2659", "8531", "4752", "9812", "5763", "3498", "6781", "9123", "1245", "6537",
        "3091", "7845", "2893", "6534", "1829", "9876", "5432", "1234", "9087", "7654", "2345",
        "8765", "5432", "2134", "7865", "1249", "4301", "8976", "2098", "6234", "4567", "9564",
        "7532", "9987", "5703", "8765", "9284", "6092", "4531", "8635", "4398", "7612", "2089",
        "6152", "3546", "8790", "9843", "5261", "4215", "6714", "3156", "1065", "7834",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "-347", "-659", "-531", "-752", "-812", "-763", "-498", "-781", "-123", "-245", "-537",
        "-091", "-845", "-893", "-534", "-829", "-876", "-432", "-234", "-087", "-654", "-345",
        "-765", "-432", "-134", "-865", "-249", "-301", "-976", "-098", "-234", "-567", "-564",
        "-532", "-987", "-703", "-765", "-284", "-092", "-531", "-635", "-398", "-612", "-089",
        "-152", "-546", "-790", "-843", "-261", "-215", "-714", "-156", "-065", "-834",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "0047", "0059", "0031", "0052", "0012", "0063", "0098", "0081", "0023", "0045", "0037",
        "0091", "0045", "0093", "0034", "0029", "0076", "0032", "0034", "0087", "0054", "0045",
        "0065", "0032", "0034", "0065", "0049", "0001", "0076", "0098", "0034", "0067", "0064",
        "0032", "0087", "0003", "0065", "0084", "0092", "0031", "0035", "0098", "0012", "0089",
        "0052", "0046", "0090", "0043", "0061", "0015", "0014", "0056", "0065", "0034",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "0000", "1111", "2222", "3333", "4444", "5555", "6666", "7777", "8888", "9999", "1234",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "qX2t", "pY9s", "mZ7r", "kA3v", "nB6w", "eC8x", "fD1y", "gE5z", "hF4u", "iG0t", "jH6q",
        "rI3w", "tJ8e", "yK1r", "uL4t", "wM7y", "oN9u", "zO2i", "xP5o", "vQ0p", "aR7s", "bS3d",
        "cT6f", "dU1g", "eV4h", "fW8j", "gX5k", "hY2l", "iZ9m", "jA6n", "kB0p", "lC7q", "mD3r",
        "nE5s", "oF1t", "pG4u", "qH7v", "rI2w", "sJ9x", "tK6y",
    ];
    keys_exist!(lsm, keys, false);
}

#[test]
fn lsm_merge_tombstones_single() {
    let test_path = "./test-data/lsm-merge-tombstones-single";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::<SF>::new(0.1, 10, String::from(test_path), 3, 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, false, "").unwrap();

    let keys = vec![
        "456", "789", "234", "567", "890", "901", "345", "678", "123", "432", "765", "210", "543",
        "876", "109", "987", "654", "321", "345", "678", "901", "234", "567", "890", "123", "456",
        "789", "432", "765", "210", "543", "876", "109", "987", "654", "321", "345", "678", "901",
        "234", "567", "890", "123",
    ];

    keys_exist!(lsm, keys.clone(), true);
    are_tombstones!(lsm, keys, false);

    // tombstones
    insert_range(&mut (501..600), test_path, &mut lsm, true, false, "").unwrap();

    lsm.merge(0, test_path).unwrap();

    let keys = vec![
        "501", "589", "534", "567", "590", "501", "545", "578", "523", "532", "565", "510", "543",
        "576", "509", "587", "554", "521", "545", "578", "501", "534", "567", "590", "523", "556",
        "589", "532", "565", "510", "543", "576", "509", "587", "554", "521", "545", "578", "501",
        "534", "567", "590", "523", "599",
    ];

    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "0", "1", "4", "7", "3", "9", "54", "67", "12", "43", "76", "21", "54", "32", "76", "29",
        "87", "54", "21", "45", "78", "71", "34", "67", "90", "23", "56", "89", "32", "65", "10",
        "43", "76", "99", "87", "54", "21", "45", "78", "10", "34", "67", "90", "23",
    ];
    keys_exist!(lsm, keys, true);
}

#[test]
fn lsm_merge_mix_tomb_single() {
    let test_path = "./test-data/lsm-merge-mix-tomb-single";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::<SF>::new(0.1, 10, String::from(test_path), 3, 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, false, "").unwrap();

    let keys = vec![
        "456", "789", "234", "567", "890", "901", "345", "678", "123", "432", "765", "210", "543",
        "876", "109", "987", "654", "321", "345", "678", "901", "234", "567", "890", "123", "456",
        "789", "432", "765", "210", "543", "876", "109", "987", "654", "321", "345", "678", "901",
        "234", "567", "890", "123",
    ];

    keys_exist!(lsm, keys.clone(), true);
    are_tombstones!(lsm, keys, false); // this is reduntant now ?

    // tombstones for existing entries
    insert_range(&mut (501..600), test_path, &mut lsm, true, false, "applied").unwrap();

    // tombstones for fun
    insert_range(
        &mut (2001..2100),
        test_path,
        &mut lsm,
        true,
        false,
        "propagated",
    )
    .unwrap();

    lsm.merge(0, test_path).unwrap();

    let keys = vec![
        "501", "589", "534", "567", "590", "501", "545", "578", "523", "532", "565", "510", "543",
        "576", "509", "587", "554", "521", "545", "578", "501", "534", "567", "590", "523", "556",
        "589", "532", "565", "510", "543", "576", "509", "587", "554", "521", "545", "578", "501",
        "534", "567", "590", "523", "599",
    ];
    keys_exist!(lsm, keys.clone(), false);

    let keys: Vec<&str> = vec![
        "0", "1", "4", "7", "3", "9", "54", "67", "12", "43", "76", "21", "54", "32", "76", "29",
        "87", "54", "21", "45", "78", "71", "34", "67", "90", "23", "56", "89", "32", "65", "10",
        "43", "76", "99", "87", "54", "21", "45", "78", "10", "34", "67", "90", "23",
    ];
    keys_exist!(lsm, keys, true);

    // goofy tombstones
    let keys: Vec<&str> = vec![
        "2087", "2054", "2021", "2045", "2078", "2071", "2034", "2067", "2090", "2023", "2056",
        "2089", "2032", "2065", "2010", "2043", "2076", "2099", "2087", "2054", "2021", "2045",
        "2078", "2010", "2034", "2067", "2090", "2023",
    ];

    // keys_exist!(false) == doesn't exist, or is tombstone because
    // both failed searches and found tombstones return None
    keys_exist!(lsm, keys.clone(), false);

    assert_eq!(lsm.get(Vec::from("2002")), None);

    assert_eq!(lsm.get(Vec::from("2012")), None);

    assert_eq!(lsm.get(Vec::from("2021")), None);
}

#[test]
fn lsm_merge_mix_tomb_auto_single() {
    let test_path = "./test-data/lsm-merge-mix-tomb-auto-single";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::<SF>::new(0.1, 10, String::from(test_path), 3, 3);

    insert_range(&mut (0..700), test_path, &mut lsm, false, true, "").unwrap();

    let keys = vec![
        "456", "469", "234", "567", "690", "421", "345", "648", "123", "432", "465", "210", "543",
        "646", "109", "687", "654", "321", "345", "648", "301", "234", "567", "690", "123", "456",
        "469", "432", "465", "210", "543", "646", "109", "267", "654", "321", "345", "648", "301",
        "234", "567", "690", "123",
    ];

    keys_exist!(lsm, keys.clone(), true);

    // tombstones for existing keys
    insert_range(&mut (201..300), test_path, &mut lsm, true, true, "applied").unwrap();

    // tombstones for fun
    insert_range(
        &mut (2001..2100),
        test_path,
        &mut lsm,
        true,
        true,
        "propagated",
    )
    .unwrap();

    lsm.levels
        .iter()
        .enumerate()
        .map(|(idx, l)| {
            (
                idx,
                l.nodes.iter().map(|n| n.path.clone()).collect::<Vec<_>>(),
            )
        })
        .for_each(|p| println!("STATUS {} {:?}", p.0, p.1));

    // deleted
    let keys = vec![
        "201", "289", "234", "267", "290", "201", "242", "278", "223", "232", "262", "210", "243",
        "276", "209", "287", "224", "221", "242", "278", "201", "234", "267", "290", "223", "226",
        "289", "232", "262", "210", "243", "276", "209", "287", "224", "221", "242", "278", "201",
        "234", "267", "290", "223", "299",
    ];
    keys_exist!(lsm, keys.clone(), false);

    // kept
    let keys: Vec<&str> = vec![
        "0", "1", "4", "7", "3", "9", "54", "67", "12", "43", "76", "21", "54", "32", "76", "29",
        "87", "54", "21", "45", "78", "71", "34", "67", "90", "23", "56", "89", "32", "65", "10",
        "43", "76", "99", "87", "54", "21", "45", "78", "10", "34", "67", "90", "23",
    ];
    keys_exist!(lsm, keys, true);

    // goofy tombstones
    let keys: Vec<&str> = vec![
        "2087", "2054", "2021", "2045", "2078", "2071", "2034", "2067", "2090", "2023", "2056",
        "2089", "2032", "2065", "2010", "2043", "2076", "2099", "2087", "2054", "2021", "2045",
        "2078", "2010", "2034", "2067", "2090", "2023",
    ];

    keys_exist!(lsm, keys.clone(), false);

    assert_eq!(lsm.get(Vec::from("2002")), None);

    assert_eq!(lsm.get(Vec::from("2012")), None);

    assert_eq!(lsm.get(Vec::from("2021")), None);
}



#[test]
fn lsm_load_single() -> Result<()> {
    let test_path = "./test-data/lsm-load-single";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::<SF>::new(0.1, 10, String::from(test_path), 3, 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, true, "").unwrap();

    lsm.levels
        .iter()
        .enumerate()
        .map(|(idx, l)| {
            (
                idx,
                l.nodes.iter().map(|n| n.path.clone()).collect::<Vec<_>>(),
            )
        })
        .for_each(|p| println!("STATUS {} {:?}", p.0, p.1));
    
        println!("CLEARIND");
    lsm.levels.clear();
        println!("CLEARED");

    lsm.levels
        .iter()
        .enumerate()
        .map(|(idx, l)| {
            (
                idx,
                l.nodes.iter().map(|n| n.path.clone()).collect::<Vec<_>>(),
            )
        })
        .for_each(|p| println!("STATUS {} {:?}", p.0, p.1));

        println!("LOADING... ");

    lsm.load().context("loading data")?;
        println!("LOADED ");

    println!("lelvls {:?}", lsm.levels);

    lsm.levels
        .iter()
        .enumerate()
        .map(|(idx, l)| {
            (
                idx,
                l.nodes.iter().map(|n| n.path.clone()).collect::<Vec<_>>(),
            )
        })
        .for_each(|p| println!("STATUS {} {:?}", p.0, p.1));

    let keys: Vec<&str> = vec![
        "456", "789", "234", "567", "890", "901", "345", "678", "123", "432", "765", "210", "543",
        "876", "109", "987", "654", "321", "345", "678", "901", "234", "567", "890", "123", "456",
        "789", "432", "765", "210", "543", "876", "109", "987", "654", "321", "345", "678", "901",
        "234", "567", "890", "123",
    ];
    keys_exist!(lsm, keys, true);

    Ok(())
}
