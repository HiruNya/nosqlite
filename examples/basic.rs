use nosqlite::{Connection, Entry};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct User {
	name: String,
	age: u8,
}

fn main() {
	// If no database with the name provided exists, it is created.
	let connection = Connection::open("./test.db").unwrap();
	// If no table within the database exists with the name, it is created.
	let table = connection.table("test").unwrap();
	// Inserts a Json object into the table.
	table.insert(User { name: "Hiruna".into(), age: 19}, &connection).unwrap();
	table.insert(User { name: "Bob".into(), age: 13}, &connection).unwrap();
	// Gets the first Json object in the table. (Not the one we just inserted unless the table was empty).
	let data: Entry<i64, User> = table.get(1).entry(&connection).unwrap().unwrap();
	println!("{:?}", data)
}