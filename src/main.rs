use katana_db::{
    abstraction::{Database, DbCursor, DbTx},
    open_db, tables,
};

fn main() {
    let default_path = "/data".to_string();

    let args = std::env::args().collect::<Vec<String>>();
    let path = args.get(1).unwrap_or(&default_path);

    let mut total_txs: u64 = 0;
    let db = open_db(path).unwrap();

    let tx = db.tx().unwrap();
    if let Some(res) = tx.cursor::<tables::TxNumbers>().unwrap().last().unwrap() {
        let (.., tx_number) = res;
        total_txs += tx_number;
    }

    println!("{total_txs}");
}
