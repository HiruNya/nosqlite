use nosquirrelite::{Connection, Entry};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct User {
	first_name: String,
	last_name: String,
	age: u8,
}

fn main() {
	let connection = Connection::open("../hotpot-test/database.hpdb").unwrap();
	let table = connection.table("test").unwrap();
	let data: Entry<User, _> = table.get(1).entry(&connection).unwrap().unwrap();
	println!("{:?}", data)
}