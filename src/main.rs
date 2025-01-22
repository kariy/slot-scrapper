use katana_db::{
    abstraction::{Database, DbCursor, DbTx},
    open_db, tables,
};

fn main() {
    let default_path = "/data".to_string();

    let args = std::env::args().collect::<Vec<String>>();
    let path = args.get(1).unwrap_or(&default_path);

    let db = open_db(path).unwrap();

    let tx = db.tx().unwrap();
    let total = tx.entries::<tables::Transactions>().unwrap();
    println!("{total}");
}
