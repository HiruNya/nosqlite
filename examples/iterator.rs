use nosqlite::{Connection, field};
use serde::Serialize;

#[derive(Serialize)]
struct User {
	name: String,
	age: u8,
}

fn main() {
	let connection = Connection::in_memory().unwrap();
	let table = connection.table("people").unwrap();
	table.insert(&User{ name: "Hiruna".into(), age: 18 }, &connection).unwrap();
	table.insert(&User{ name: "Bob".into(),  age: 13 }, &connection).unwrap();
	table.insert(&User{ name: "Alex".into(), age: 20 }, &connection).unwrap();
	table.insert(&User{ name: "Callum".into(), age: 25 }, &connection).unwrap();
	// Iterate over the entries in the table
	table.iter()
		// Only get people who are 18+
		.filter(field("age").gte(18))
		// Gets the name and age fields of the JSON object
		.fields::<(String, u8), _, _, _>(&["name", "age"], &connection)
		.unwrap()
		.into_iter()
		.for_each(|(name, age)| println!("{:10} : {}", name, age));
}