use clap::load_yaml;
use clap::App;
use std::env;
use std::process;

fn main() {
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

    match app_m.subcommand() {
        ("get", Some(sub_m)) => {
            if let Some(_) = sub_m.value_of("key") {
                eprintln!("unimplemented");
                process::exit(1);
            } else {
                app_m.usage();
                process::exit(1);
            }
        }
        ("set", Some(sub_m)) => {
            if let (Some(_), Some(_)) = (sub_m.value_of("key"), sub_m.value_of("value")) {
                eprintln!("unimplemented");
                process::exit(1);
            } else {
                app_m.usage();
                process::exit(1);
            }
        }
        ("rm", Some(sub_m)) => {
            if let Some(_) = sub_m.value_of("key") {
                eprintln!("unimplemented");
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
