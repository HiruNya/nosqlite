use nosqlite::{Connection, field};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct User {
	first_name: String,
	last_name: String,
	age: u8,
}

fn main() {
	// If no database with the name provided exists, it is created.
	let connection = Connection::open("./test.db").unwrap();
	// If no table within the database exists with the name, it is created.
	let table = connection.table("test").unwrap();
	let data: Vec<(String, u8)> = table.iter()
		.filter(field("age").gte(18))
		.skip(0)
		.take(2)
		.fields(&["first_name", "age"], &connection)
		.unwrap();
	// Inserts a Json object into the table.
	// table.insert(User { first_name: "Hiruna".into(), last_name: "Jayamanne".into(), age: 19}, &connection)
	// 	.unwrap();
	// Gets the first Json object in the table. (Not the one we just inserted unless the table was empty).
	// let data: Entry<User, _> = table.get(1).entry(&connection).unwrap().unwrap();
	println!("{:?}", data)
}