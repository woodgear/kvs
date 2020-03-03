use structopt;
use structopt::StructOpt;

use kvs::{KvStore, Result};

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs", about = "an simple in memory kv db")]
struct Opt {
    #[structopt(subcommand)]
    subcmd: SubCmd,
}

#[derive(Debug, StructOpt)]
enum SubCmd {
    Get { key: String },
    Set { key: String, val: String },
    Rm { key: String },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let mut store = KvStore::open(std::env::current_dir()?)?;
    match opt.subcmd {
        SubCmd::Get { key } => match store.get(key)? {
            Some(v) => {
                println!("{}", v);
            }
            None => {
                println!("Key not found");
            }
        },
        SubCmd::Set { key, val } => {
            store.set(key, val)?;
        }
        SubCmd::Rm { key } => match store.remove(key) {
            Ok(_) => {}
            Err(e) => {
                println!("Key not found");
                std::process::exit(-1);
            }
        },
    };
    Ok(())
}
