use clap::load_yaml;
use clap::App;
use std::env;
use std::process;
use kvs::{KvStore, KvsResult};

fn main() -> KvsResult<()> {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    if app_m.is_present("version") {
        let key = "CARGO_PKG_VERSION";
        match env::var(key) {
            Ok(val) => println!("{:?}", val),
            Err(e) => println!("couldn't interpret {}: {}", key, e),
        }

        process::exit(0);
    }

    let mut store = KvStore::open("./data")?;

    match app_m.subcommand() {
        ("get", Some(sub_m)) => {
            if let Some(key) = sub_m.value_of("key") {
                if let Some(value) = store.get(key)? {
                    println!("{}", value);
                } else {
                    println!("key not found");
                }
                process::exit(1);
            } else {
                app_m.usage();
                process::exit(1);
            }
        }
        ("set", Some(sub_m)) => {
            if let (Some(key), Some(value)) = (sub_m.value_of("key"), sub_m.value_of("value")) {
                store.set(key.to_string(), value.to_string())?;
                process::exit(1);
            } else {
                app_m.usage();
                process::exit(1);
            }
        }
        ("rm", Some(sub_m)) => {
            if let Some(key) = sub_m.value_of("key") {
                store.remove(key)?;
                process::exit(1);
            } else {
                app_m.usage();
                process::exit(1);
            }
        }
        _ => {
            app_m.usage();
            process::exit(1);
        }
    }
}
