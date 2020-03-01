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
    match opt.subcmd {
        SubCmd::Get { .. } => {
            panic!("unimplemented");
        }
        SubCmd::Set { .. } => {
            panic!("unimplemented");
        }
        SubCmd::Rm { .. } => {
            panic!("unimplemented");
        }
    }
    Ok(())
}
