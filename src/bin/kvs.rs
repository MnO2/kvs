use clap::App;
use clap::load_yaml;

fn main() {
    let yaml = load_yaml!("cli.yml");
    let app_m = App::from_yaml(yaml).get_matches();

    match app_m.subcommand_name() {
        Some("get") => {}, 
        Some("set") => {},
        Some("remove") => {},
        _ => {}
    }
}
