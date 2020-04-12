//! This crate implements a NoSQL-like API over SQLite using SQLite's Json1 extension.
//!
//! If this is your first time here, start off by reading the [`Connection`] docs.
//!
//! [`Connection`]: struct.Connection.html
#![warn(missing_docs)]
#![allow(clippy::tabs_in_doc_comments)]

use rusqlite::{Connection as SqliteConnection, Error as SqliteError, NO_PARAMS, OptionalExtension,
				Result as SqliteResult, Row,
				types::{FromSqlError, FromSqlResult, ToSqlOutput, Value, ValueRef}};
use serde::{Deserialize, de::DeserializeOwned, Serialize};
use serde_json::to_string;

use std::{fmt::Display, marker::{PhantomData, Sized}, path::Path};

mod iterator;
pub use iterator::Iterator;
pub mod util;
pub(crate) use util::*;

pub use rusqlite::types::{FromSql, ToSql};
pub use serde_json::json;

/// A connection the underlying sqlite database.
pub struct Connection {
	connection: SqliteConnection,
}
impl Connection {
	/// Opens a connection to a sqlite database.
	///
	/// Creates one if it doesn't exist.
	///
	/// # Example
	///
	/// ```no_run
	/// use nosqlite::Connection;
	/// let connection = Connection::open("database.db")?;
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn open<P: AsRef<Path>>(path: P) -> SqliteResult<Self> {
		Ok(Self { connection: SqliteConnection::open(path)? })
	}

	/// Opens a new connection to a sqlite database in-memory.
	///
	/// # Example
	///
	/// ```rust
	/// use nosqlite::Connection;
	/// let connection = Connection::in_memory()?;
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn in_memory() -> SqliteResult<Self> {
		Ok(Self { connection: SqliteConnection::open_in_memory()? })
	}

	/// Gets a table in the database using its name.
	///
	/// Creates one if it doesn't exist.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, Table};
	/// # let connection = Connection::in_memory()?;
	/// let table = connection.table("people")?;
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn table<T: Into<String>>(&self, table: T) -> SqliteResult<Table<i64>> {
		let table = table.into();
		self.connection.execute(&format!(r#"
			CREATE TABLE IF NOT EXISTS {} (
				id INTEGER PRIMARY KEY,
				data TEXT NOT NULL
			)
		"#, table), NO_PARAMS)
			.map(move |_| Table {
				id: "id".into(),
				id_type: PhantomData::default(),
				data: "data".into(),
				name: table,
			})
	}
}
impl AsRef<SqliteConnection> for Connection {
	fn as_ref(&self) -> &SqliteConnection {
		&self.connection
	}
}

/// A table in the database.
///
/// Use [`Connection::table`] to make this struct but if you have a custom schema,
/// you should make this struct by providing the fields your self.
///
/// The generic type parameter, I, is the type of the Id column.
pub struct Table<I> {
	/// The id column.
	///
	/// The default implementation uses an integer but the value does not matter.
	/// e.g. a String UUID can be used.
	pub id: String,
	id_type: PhantomData<fn() -> I>, // The type of the id.
	/// The column that stores the JSON object.
	///
	/// The default implementation uses a Non Null Text but the value can be nullable or a Blob.
	pub data: String,
	/// The name of the table.
	pub name: String,
}
impl<A> Table<A> {
	/// Creates an index on the table with the given keys.
	///
	/// This is meant to speed up queries but whether it is actually used or not is determined
	/// by SQLite at runtime so I would suggest running some sort of benchmark to see if creating
	/// the index actually does speed up query times.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{column, Connection, field};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.index("my_index", &[field("name"), field("age")], connection)?;
	/// # rusqlite::Result::Ok(())
	/// ```
	///
	/// If you want to index both a field and a column then you need to cast the reference
	/// to a `&dyn Key`.
	/// ```
	/// # use nosqlite::{column, Connection, field, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.index("my_index", &[&field("name") as &dyn Key, &column("id") as &dyn Key], connection)?;
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn index<S, I, T, C>(&self, name: S, keys: I, connection: C) -> SqliteResult<()>
		where
			S: Display,
			I: IntoIterator<Item=T>,
			T: Key,
			C: AsRef<SqliteConnection>,
	{
		let keys = keys.into_iter().map(|k| k.key(&self.data))
			.fold(String::new(), |mut s, k| {
				if !s.is_empty() {
					s.push(',');
				}
				s.push_str(k.as_str());
				s
			});
		connection.as_ref().prepare(&format!("CREATE INDEX {} ON {} ({})", name, self.name, keys))?
			.execute(NO_PARAMS).map(|_|())
	}
}
impl<I: FromSql> Table<I> {
	/// Creates a table but doesn't check if the table exists.
	///
	/// This can be used to give a custom name and type of the id and data columns.
	///
	/// Generally using [`Connection::table`] is recommended instead of this.
	pub fn unchecked<K: FromSql, T: Into<String>>(id: T, data: T, name: T) -> Self {
		Self {
			id: id.into(),
			data: data.into(),
			name: name.into(),
			id_type: PhantomData::default(),
		}
	}

	/// Iterate through all the entries in the table.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, field, json, Key, Table};
	/// # let connection = Connection::in_memory()?;
	/// let table = connection.table("people")?;
	/// table.insert(json!({"name": "Hiruna", "age": 19}), &connection)?;
	/// table.insert(json!({"name": "Bobby", "age": 13}), &connection)?;
	/// let data = table.iter()
	/// 	.filter(field("age").gte(18))
	/// 	.fields::<(String, u8), _, _, _>(&["name", "age"], &connection)?;
	/// assert_eq!(data.len(), 1);
	/// assert!(data[0].1 > 18);
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn iter(&self) -> Iterator<I, (), ()> {
		Iterator {
			data_key: &self.data,
			id_key: &self.id,
			id_type: self.id_type,
			limit: None,
			offset: None,
			order_by: (),
			table_key: &self.name,
			where_: (),
		}
	}

	/// Inserts a JSON object into the data column of the table.
	///
	/// Multiple JSON objects that are exactly the same can be inserted.
	///
	/// **Warning**: If your table has other columns that are not nullable, then you should not use this.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, json, Table};
	/// # let connection = Connection::in_memory()?;
	/// // This table is empty
	/// let table = connection.table("people")?;
	/// // We'll insert two JSON objects which are exactly the same
	/// let json = json!({"name": "Hiruna", "age": 19});
	/// table.insert(&json, &connection)?;
	/// table.insert(&json, &connection)?;
	/// // Now we'll check how many entries the table has
	/// let length = table.iter().id(&connection)?.len();
	/// assert_eq!(length, 2);
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn insert<T: Serialize, C: AsRef<SqliteConnection>>(&self, data: T, connection: C) -> SqliteResult<()> {
		connection.as_ref().prepare(&format!("INSERT INTO {} ({}) VALUES (?)", self.name, self.data))?
			.execute(&[&Json(data)])?;
		Ok(())
	}
}
impl <I: FromSql + ToSql> Table<I> {
	/// Gets a JSON object using a id from the id column.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, json, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	name: String,
	/// }
	/// // The table of these ids are integers
	/// let table = connection.table("people")?;
	/// table.insert(&Person {name: "Hiruna".into()}, &connection)?;
	/// table.insert(&Person {name: "Bobby".into()}, &connection)?;
	/// // Now we'll get the 2nd entry from the table.
	/// let bobby: Person = table.get(2).data(&connection)?.unwrap();
	/// assert_eq!(bobby.name, "Bobby");
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn get(&self, id: I) -> Operation<I> {
		Operation { id, data_key: &self.data, id_key: &self.id, table: &self.name }
	}

	/// Deletes an entry with the given primary key.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, json, Table};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// // We insert 2 entries
	/// table.insert(json!(1), &connection)?;
	/// table.insert(json!(2), &connection)?;
	/// let length = table.iter().id(&connection)?.len();
	/// assert_eq!(length, 2);
	/// // Remove the entry with id 1
	/// table.delete(1, &connection)?;
	/// // Table should only have entry now
	/// let length = table.iter().id(&connection)?.len();
	/// assert_eq!(length, 1);
	/// // And we shouldn't be able to access the entry with primary key 1 now
	/// assert!(table.get(1).id(&connection)?.is_none());
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn delete<C: AsRef<SqliteConnection>>(&self, id: I, connection: C) -> SqliteResult<()> {
		connection.as_ref().execute(
			&format!("DELETE FROM {} WHERE {} = ?", self.name, self.id),
			&[&id],
		).map(|_|())
	}
}

fn format_key(key: &str) -> String {
	let mut chars = key.chars();
	let mut prepend = String::with_capacity(2+key.len());
	if chars.next() != Some('$') {
		prepend.push('$');
		let c = chars.next();
		if c != Some('.') && (c.is_some() || key.len() == 1) {
			prepend.push('.');
		}
	}
	prepend.push_str(key);
	prepend
}

/// Represents an operation to get a JSON object using its id key.
#[must_use = "This struct must be used for the database to be queried."]
pub struct Operation<'a, I: FromSql + ToSql> {
	data_key: &'a str,
	id: I,
	id_key: &'a str,
	table: &'a str,
}
impl<'a, I: FromSql + ToSql> Operation<'a, I> {
	/// Gets only the JSON object, deserialising it into the struct provided.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	name: String,
	/// }
	/// // Assume empty table
	/// table.insert(Person{ name: "Hiruna".into() }, &connection)?;
	/// table.insert(Person{ name: "Bobby".into() }, &connection)?;
	/// let bobby: Person = table.get(2).data(&connection)?.unwrap();
	/// assert_eq!(bobby.name, "Bobby");
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn data<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Option<T>> {
		connection.as_ref().query_row(
			&format!("SELECT {} FROM {} WHERE {} = ?", self.data_key, self.table, self.id_key),
			&[&self.id],
			|row| row.get(0)
		).map(Json::unwrap).optional()
	}
	/// Gets both the id and the JSON object.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, Entry, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	name: String,
	/// }
	/// // Assume empty table
	/// table.insert(Person{ name: "Hiruna".into() }, &connection)?;
	/// table.insert(Person{ name: "Bobby".into() }, &connection)?;
	/// let bobby: Entry<i64, Person> = table.get(2).entry(&connection)?.unwrap();
	/// assert_eq!(bobby.data.name, "Bobby");
	/// assert_eq!(bobby.id, 2);
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn entry<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Option<Entry<I, T>>> {
		connection.as_ref().query_row(
			&format!("SELECT {}, {} FROM {} WHERE {} = ?", self.id_key, self.data_key, self.table, self.id_key),
			&[&self.id],
			Entry::from_row
		).optional()
	}
	/// Gets only the id of the entry.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, Entry, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	name: String,
	/// }
	/// // Assume empty table
	/// table.insert(Person{ name: "Hiruna".into() }, &connection)?;
	/// table.insert(Person{ name: "Bobby".into() }, &connection)?;
	/// let bobby_id: i64 = table.get(2).id(&connection)?.unwrap();
	/// assert_eq!(bobby_id, 2);
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn id<C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Option<I>> {
		connection.as_ref().query_row(
			&format!("SELECT {} FROM {} WHERE {} = ?", self.id_key, self.table, self.id_key),
			&[&self.id],
			|row| row.get(0)
		).optional()
	}
	/// Extracts a possibly nested field in the JSON object.
	///
	/// # Example
	///
	/// ```rust
	/// # use nosqlite::{Connection, Entry, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	name: String,
	/// }
	/// // Assume empty table
	/// table.insert(Person{ name: "Hiruna".into() }, &connection)?;
	/// table.insert(Person{ name: "Bobby".into() }, &connection)?;
	/// let bobby: String = table.get(2).field("name", &connection)?.unwrap();
	/// assert_eq!(bobby, "Bobby");
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn field<T: FromSql, C: AsRef<SqliteConnection>>(&self, key: &str, connection: C) -> SqliteResult<Option<T>> {
		let key = format_key(key);
		connection.as_ref().query_row(
			&format!("SELECT json_extract({}, \"{}\") FROM {} WHERE {} = ?", self.data_key, key, self.table, self.id_key),
			&[&self.id],
			|row| row.get(0)
		).optional()
	}

	/// Removes a *field* from a JSON object.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, Table, json};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// table.insert(json!({ "name": "Hiruna", "age": 19 }), &connection)?;
	/// table.insert(json!({ "name": "Bob", "age": 13 }), &connection)?;
	///
	/// table.get(2).remove("age", &connection);
	/// // Only 1 entry is returned because the second JSON object doesn't have an `age` field
	/// // so only entry 1 has an age field
	/// let people: Vec<(String, u8)> = table.iter().fields(&["name", "age"], &connection)?;
	/// assert_eq!(people.len(), 1);
	/// assert_eq!(people[0].0, "Hiruna");
	/// // This *does not* delete the entry
	/// assert_eq!(table.iter().id(&connection)?.len(), 2);
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn remove<C>(&self, field: &str, connection: C) -> SqliteResult<()>
		where C: AsRef<SqliteConnection>
	{
		let path = format_key(field);
		let set_value = format!("{} = json_remove({}, '{}')", self.data_key, self.data_key, path);
		connection.as_ref().execute(
			&format!("UPDATE {} SET {} WHERE {} = ?", self.table, set_value, self.id_key),
			&[&self.id]
		).map(|_|())
	}
}

/// This can be used for filters or getting fields
pub trait Key {
	/// Produces the string that will be used by SQL.
	fn key(&self, data_key: &str) -> String;

	/// Compares for equality.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is equal to 3
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").eq(3)).field("number", &connection)?;
	/// // Only 2 entries should've matched the filter
	/// assert_eq!(numbers.len(), 2);
	/// // They both should be equal to 3
	/// assert_eq!(numbers.into_iter().any(|number| number != 3), false);
	/// # rusqlite::Result::Ok(())
	/// ```
	fn eq<T: Serialize>(self, value: T) -> Eq<Self, String>
	where Self: Sized {
		Eq { variable: self, value: to_string(&value).unwrap() }
	}

	/// Compares for inequality.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is not equal to 3
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").neq(3)).field("number", &connection)?;
	/// // 3 entries should've matched the filter
	/// assert_eq!(numbers.len(), 3);
	/// // They both should be equal to 3
	/// assert!(!numbers.into_iter().any(|number| number == 3));
	/// # rusqlite::Result::Ok(())
	/// ```
	fn neq<T: Serialize>(self, value: T) -> Neq<Self, String>
		where Self: Sized {
		Neq { variable: self, value: to_string(&value).unwrap() }
	}
	/// Compares if it is greater than the value.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is greater than 4
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").gt(4)).field("number", &connection)?;
	/// // Only 9 is bigger than 4
	/// assert_eq!(numbers.len(), 1);
	/// assert!(numbers[0] > 4);
	/// # rusqlite::Result::Ok(())
	/// ```
	fn gt<T: Serialize>(self, value: T) -> Gt<Self, String>
	where Self: Sized{
		Gt { greater: self, lesser: to_string(&value).unwrap() }
	}
	/// Compares if it is greater than or equal to the value.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is greater than or equal to 4
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").gte(4)).field("number", &connection)?;
	/// assert_eq!(numbers.len(), 2);
	/// assert!(!numbers.into_iter().any(|number| number < 4));
	/// # rusqlite::Result::Ok(())
	/// ```
	fn gte<T: Serialize>(self, value: T) -> Gte<Self, String>
	where Self: Sized {
		Gte { greater: self, lesser: to_string(&value).unwrap() }
	}
	/// Compares if it is less than the value.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is less than 4
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").lt(4)).field("number", &connection)?;
	/// assert_eq!(numbers.len(), 3);
	/// assert!(!numbers.into_iter().any(|number| number >= 4));
	/// # rusqlite::Result::Ok(())
	/// ```
	fn lt<T: Serialize>(self, value: T) -> Gt<String, Self>
	where Self: Sized {
		Gt { lesser: self, greater: to_string(&value).unwrap() }
	}
	/// Compares if it is greater than or equal to the variable.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// table.insert(json!({"number": 9}), &connection)?;
	/// table.insert(json!({"number": 2}), &connection)?;
	/// table.insert(json!({"number": 4}), &connection)?;
	/// table.insert(json!({"number": 3}), &connection)?;
	/// // Get only the entries where the number is less than or equal to 4
	/// let numbers: Vec<u8> = table.iter()
	/// 	.filter(field("number").lte(4)).field("number", &connection)?;
	/// assert_eq!(numbers.len(), 4);
	/// assert!(!numbers.into_iter().any(|number| number > 4));
	/// # rusqlite::Result::Ok(())
	/// ```
	fn lte<T: Serialize>(self, value: T) -> Gte<String, Self>
	where Self: Sized {
		Gte { lesser: self, greater: to_string(&value).unwrap() }
	}
	/// Uses the SQL like comparison operator.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"name": "Hiruna"}), &connection)?;
	/// table.insert(json!({"name": "Alex"}), &connection)?;
	/// table.insert(json!({"name": "Bob"}), &connection)?;
	/// table.insert(json!({"name": "Haruna"}), &connection)?;
	/// table.insert(json!({"name": "Felix"}), &connection)?;
	///
	/// // Get all names that end with 'x'
	/// let names: Vec<String> = table.iter()
	/// 	.filter(field("name").like(true, 'x', false)).field("name", &connection)?;
	/// // We should only match two names
	/// assert_eq!(names.len(), 2);
	///
	/// // Get all names that start with `H` and end with 'runa'
	/// let names: Vec<String> = table.iter()
	/// 	.filter(field("name").like(false, "H%runa", false)).field("name", &connection)?;
	/// // We should only match two names
	/// assert_eq!(names.len(), 2);
	/// # rusqlite::Result::Ok(())
	/// ```
	fn like<S: std::fmt::Display>(self, matches_start: bool, value: S, matches_end: bool) -> Like<Self, S>
	where Self: Sized {
		Like { variable: self, matches_start, value, matches_end }
	}

	/// Whether the value exists in the JSON object and if it does exist, whether it is not null.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"a": 3, "b": 20}), &connection)?;
	/// table.insert(json!({"b": 3, "c": 20}), &connection)?;
	/// table.insert(json!({"a": 89, "b": 3, "c": 20}), &connection)?;
	/// table.insert(json!({"a": null, "b": 3, "c": 20}), &connection)?;
	///
	/// let ids: Vec<i64> = table.iter().filter(field("a").exists()).id(&connection)?;
	/// // Only Entry 1 and 3 should have passed the filter
	/// // Entry 2 doesn't have the `a` field and Entry 4 has an `a` field but it's `null`
	/// // Therefore only two entries should have passed
	/// assert_eq!(ids.len(), 2);
	/// assert_eq!(ids[0], 1);
	/// assert_eq!(ids[1], 3);
	/// # rusqlite::Result::Ok(())
	/// ```
	fn exists(self) -> Exists<Self> where Self: Sized { Exists(self) }

	/// This field is to be sorted in ascending order.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key, Table};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"a": 4}), &connection)?;
	/// table.insert(json!({"a": 8}), &connection)?;
	/// table.insert(json!({"a": 6}), &connection)?;
	///
	/// let a: Vec<u8> = table.iter().sort(field("a").ascending()).field("a", &connection)?;
	///
	/// assert_eq!(a[0], 4);
	/// assert_eq!(a[1], 6);
	/// assert_eq!(a[2], 8);
	/// # rusqlite::Result::Ok(())
	/// ```
	fn ascending(self) -> SortOrder<Self> where Self: Sized { SortOrder::Ascending(self) }

	/// This field is to be sorted in descending order.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key, Table};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"a": 4}), &connection)?;
	/// table.insert(json!({"a": 8}), &connection)?;
	/// table.insert(json!({"a": 6}), &connection)?;
	///
	/// let a: Vec<u8> = table.iter().sort(field("a").descending()).field("a", &connection)?;
	///
	/// assert_eq!(a[0], 8);
	/// assert_eq!(a[1], 6);
	/// assert_eq!(a[2], 4);
	/// # rusqlite::Result::Ok(())
	/// ```
	fn descending(self) -> SortOrder<Self> where Self: Sized { SortOrder::Descending(self) }
}
impl<K: Key + ?Sized> Key for &K {
	fn key(&self, data_key: &str) -> String { (*self).key(data_key) }
}

/// Represents a field in a JSON object.
///
/// Create this using the [`field`] method.
///
/// [`field`]: fn.field.html
pub struct Field(pub String);
/// Creates a representation of a field in a JSON object.
///
/// If the string is empty, the root is assumed.
pub fn field(field: &str) -> Field { Field(format_key(field)) }
impl Key for Field {
	fn key(&self, data_key: &str) -> String {
		format!("json_extract({}, \"{}\")", data_key, self.0)
	}
}

/// A column in the SQL table.
///
/// Create this using the [`column`] function.
///
/// This isn't generally used unless you have a custom table.
///
/// # Example
///
/// ```
/// # use nosqlite::{column, Column, Connection, json, Key, Table};
/// # let connection = Connection::in_memory()?;
/// # let table = connection.table("test")?;
/// table.insert(10, &connection)?;
/// table.insert(20, &connection)?;
/// table.insert(30, &connection)?;
/// // Get the entries with an id greater than 1
/// let data = table.iter().filter(column("id").gt(1)).id(&connection)?;
/// // Only 2 entries should have been queried
/// assert_eq!(data.len(), 2);
/// assert_eq!(data.into_iter().any(|id| id == 1), false);
/// # rusqlite::Result::Ok(())
/// ```
///
/// [`column`]: fn.column.html
pub struct Column(pub String);
/// Create a representation of a column in a SQL table.
pub fn column<S: Into<String>>(column: S) -> Column { Column(column.into()) }
impl Key for Column {
	fn key(&self, _: &str) -> String { self.0.clone() }
}

/// Represents a condition which will determine what entries the operation can work on.
pub trait Filter {
	/// Returns a string formatted for use in an SQL statement.
	fn where_(&self, _: &str) -> Option<String>;
	/// Allows chaining of multiple conditions.
	fn and<B: Filter>(self, second: B) -> And<Self, B>
	where Self: std::marker::Sized
	{
		And { first: self, second }
	}
	/// Allows another possible condition.
	fn or<B: Filter>(self, second: B) -> Or<Self, B>
	where Self: std::marker::Sized
	{
		Or { first: self, second }
	}
	/// Negates the condition.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, field, Key, Filter, Table};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(1, &connection)?;
	/// table.insert(2, &connection)?;
	/// table.insert(3, &connection)?;
	/// table.insert(1, &connection)?;
	///
	/// // Use a filter that effectively is a not equal comparison
	/// let numbers: Vec<u8> = table.iter().filter(field("").eq(1).not()).data(&connection)?;
	/// assert_eq!(numbers.len(), 2);
	/// assert!(!numbers.into_iter().any(|number| number == 1));
	/// # rusqlite::Result::Ok(())
	/// ```
	fn not(self) -> Not<Self> where Self: Sized { Not(self) }
}
impl Filter for () {
	fn where_(&self, _: &str) -> Option<String> { None }
}
impl Filter for String {
	fn where_(&self, _: &str) -> Option<String> { Some(self.clone()) }
}
impl<A: Filter, B: Filter> Filter for And<A, B> {
	fn where_(&self, data_key: &str) -> Option<String> {
		Some(format!("({} AND {})",
			self.first.where_(data_key).unwrap_or_default(),
			self.second.where_(data_key).unwrap_or_default()))
	}
}
impl<A: Filter, B: Filter> Filter for Or<A, B> {
	fn where_(&self, data_key: &str) -> Option<String> {
		Some(format!("({} OR {})",
			self.first.where_(data_key).unwrap_or_default(),
			self.second.where_(data_key).unwrap_or_default()))
	}
}
impl<A: Filter> Filter for Not<A> {
	fn where_(&self, data_key: &str) -> Option<String> {
		Some(format!("NOT ({})", self.0.where_(data_key).unwrap_or_default()))
	}
}
impl<K: Key> Filter for Eq<K, String> {
	fn where_(&self, data_key: &str) -> Option<String> {
		Some(format!("{} = {}", self.variable.key(data_key), self.value))
	}
}
impl<K: Key> Filter for Neq<K, String> {
	fn where_(&self, data_key: &str) ->Option<String> {
		Some(format!("{} != {}", self.variable.key(data_key), self.value))
	}
}
impl<K: Key> Filter for Gt<K, String> {
	fn where_(&self, data_key: &str) -> Option<String> {
		Some(format!("{} > {}", self.greater.key(data_key), self.lesser))
	}
}
impl<K: Key> Filter for Gte<K, String> {
	fn where_(&self, data_key: &str) -> Option<String> {
		Some(format!("{} >= {}", self.greater.key(data_key), self.lesser))
	}
}
impl<K: Key> Filter for Gt<String, K> {
	fn where_(&self, data_key: &str) -> Option<String> {
		Some(format!("{} < {}", self.lesser.key(data_key), self.greater))
	}
}
impl<K: Key> Filter for Gte<String, K> {
	fn where_(&self, data_key: &str) -> Option<String> {
		Some(format!("{} <= {}", self.lesser.key(data_key), self.greater))
	}
}
impl<K: Key, S: std::fmt::Display> Filter for Like<K, S> {
	fn where_(&self, data_key: &str) -> Option<String> {
		Some(format!("{} LIKE '{}{}{}'", self.variable.key(data_key),
			if self.matches_start { "%" } else { "" },
		    self.value,
		    if self.matches_end { "%" } else { "" }))
	}
}
impl<A: Key> Filter for Exists<A> {
	fn where_(&self, data_key: &str) -> Option<String> {
		Some(format!("{} IS NOT NULL", self.0.key(data_key)))
	}
}

/// An expression that is used to sort the entries in the database
/// before it is returned to the program.
pub trait Sort {
	/// Crates a list of SQL expressions to use for sorting
	///
	/// Normal users of this crate should not need to use this at all.
	fn order_by(&self, data_key: &str) -> Vec<String>;

	/// Add to the list of SQL expressions being used for sorting.
	///
	/// The second one will be taken into account after the first one.
	fn and<B>(self, second: B) -> And<Self, B> where Self: Sized { And { first: self, second } }
}
impl Sort for () {
	fn order_by(&self, _: &str) -> Vec<String> { Vec::new() }
}
impl<K: Key> Sort for SortOrder<K> {
	fn order_by(&self, data_key: &str) -> Vec<String> { vec![self.key(data_key)]	}
}
impl<A: Sort, B: Sort> Sort for And<A, B> {
	fn order_by(&self, data_key: &str) -> Vec<String> {
		let mut first = self.first.order_by(data_key);
		first.extend(self.second.order_by(data_key));
		first
	}
}

/// Represents one 'row' of the table.
#[derive(Debug, Deserialize)]
pub struct Entry<K, V> {
	/// The id of the entry.
	pub id: K,
	/// The JSON object.
	pub data: V,
}
impl<K, V> Entry<K, V> {
	/// Gets the JSON object out of the entry.
	pub fn data(&self) -> &V {
		&self.data
	}
}
impl<K: FromSql, V: DeserializeOwned> Entry<K, V> {
	fn from_row(row: &Row) -> SqliteResult<Entry<K, V>> {
		let id = row.get(0)?;
		let data = row.get::<_, Json<V>>(1)?.unwrap();
		Ok(Entry{ id, data })
	}
}

/// A newtype to implement the [`ToSql`] and [`FromSql`] traits for a struct that implements
/// [`Serialize`] and [`Deserialize`] respectively.
#[derive(Debug, Deserialize)]
pub struct Json<T>(T);
impl<T> Json<T> {
	/// Returns the inner value.
	pub fn unwrap(self) -> T {
		let Self(data) = self;
		data
	}
}
impl<T> AsRef<T> for Json<T> {
	fn as_ref(&self) -> &T { &self.0 }
}
impl<T: DeserializeOwned> FromSql for Json<T> {
	fn column_result(value: ValueRef) -> FromSqlResult<Self> {
		match value {
			ValueRef::Blob(data) | ValueRef::Text(data) => {
				serde_json::from_value(serde_json::from_slice(data).map_err(|err| FromSqlError::Other(Box::new(err)))?)
					.map_err(|err| FromSqlError::Other(Box::new(err)))
			}
			_ => Err(FromSqlError::InvalidType),
		}
	}
}
impl<T: Serialize> ToSql for Json<T> {
	fn to_sql(&self) -> SqliteResult<ToSqlOutput> {
		let Json(data) = &self;
		Ok(ToSqlOutput::Owned(Value::Text(to_string(data).map_err(|err| SqliteError::ToSqlConversionFailure(Box::new(err)))?)))
	}
}
