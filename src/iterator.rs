use std::marker::PhantomData;

use rusqlite::{Connection as SqliteConnection, Result as SqliteResult, Statement,
	types::{FromSql, ToSql}};
use serde::{de::DeserializeOwned, Serialize};

use crate::{Entry, field, Filter, format_key, Json};

/// Represents a potential operation on a table.
#[must_use = "This struct does not do anything until executed"]
pub struct Iterator<'a, I, W> {
	pub(crate) data_key: &'a str,
	pub(crate) id_key: &'a str,
	pub(crate) id_type: PhantomData<fn() -> I>,
	pub(crate) limit: Option<u32>,
	pub(crate) offset: Option<u32>,
	pub(crate) where_: W,
	pub(crate) table_key: &'a str,
}
impl<'a, I: FromSql, W: Filter> Iterator<'a, I, W> {
	/// ***GET***s only the JSON object.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// # #[derive(Deserialize, Serialize)]
	/// # struct Person {
	/// # 	name: String,
	/// # }
	/// let people: Vec<Person> = table.iter().data(&connection)?;
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn data<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Vec<T>> {
		self.execute_get::<_, _, _>(
			&format!("SELECT {}", self.data_key),
			get_first_column(Json::unwrap),
			connection
		)
	}

	/// ***GET***s the id and the JSON object.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// # #[derive(Deserialize, Serialize)]
	/// # struct Person {
	/// # 	name: String,
	/// # }
	/// let people: Vec<Entry<i64, Person>> = table.iter().entry(&connection)?;
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn entry<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Vec<Entry<I, T>>> {
		self.execute_get::<_, _, _>(
			&format!("SELECT {}, {}", self.id_key, self.data_key),
			|mut statement, params| {
				Ok(statement.query_map_named(
					&params,
					Entry::from_row,
				)?.filter_map(Result::ok).collect::<Vec<_>>())
			},
			connection
		)
	}

	/// ***GET***s just the id of the entry.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// # #[derive(Deserialize, Serialize)]
	/// # struct Person {
	/// # 	name: String,
	/// # }
	/// let people: Vec<i64> = table.iter().id(&connection)?;
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn id<C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Vec<I>> {
		self.execute_get::<_, _, _>(
			&format!("SELECT {}", self.id_key),
			get_first_column(no_map),
			connection
		)
	}

	/// ***GET***s a field of the JSON object.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	first_name: String,
	/// 	last_name: String,
	/// 	age: u8,
	/// }
	/// table.insert(Person{ first_name: "Hiruna".into(), last_name: "Jayamanne".into(), age: 19 }, &connection)?;
	/// let people: Vec<String> = table.iter().field("first_name", &connection)?;
	/// assert_eq!(people[0], "Hiruna");
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn field<T: FromSql, C: AsRef<SqliteConnection>>(&self, field_: &str, connection: C) -> SqliteResult<Vec<T>> {
		self.execute_get::<_, _, _>(
			&format!("SELECT {}", field(field_).key(&self)),
			get_first_column(no_map),
			connection
		)
	}

	/// ***GET***s multiple fields from the JSON object.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// #[derive(Deserialize, Serialize)]
	/// struct Person {
	/// 	first_name: String,
	/// 	last_name: String,
	/// 	age: u8,
	/// }
	/// table.insert(Person{ first_name: "Hiruna".into(), last_name: "Jayamanne".into(), age: 19 }, &connection)?;
	/// let people: Vec<(String, String)> = table.iter().fields(&["first_name", "last_name"], &connection)?;
	/// assert_eq!(people[0], ("Hiruna".into(), "Jayamanne".into()));
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn fields<T, F, C, S>(&self, fields: F, connection: C) -> SqliteResult<Vec<T>>
	where
		F: IntoIterator<Item=S>,
		S: AsRef<str>,
		T: DeserializeOwned,
		C: AsRef<SqliteConnection>,
	{
		let fields = fields.into_iter()
			.map(|s| format_key(s.as_ref()))
			.fold(String::new(), |mut init, field| {
				init.push_str(",\"");
				init.push_str(field.as_str());
				init.push('"');
				init
			});
		self.execute_get::<_, _, _>(
			&format!("SELECT json_extract({}{})", self.data_key, fields),
			get_first_column(Json::unwrap),
			connection
		)
	}

	/// Inserts a field into the JSON object with a given value.
	///
	/// If the field already exists, nothing will happen.
	/// If you wish for it to be overwritten, use [`set`] instead.
	///
	/// [`set`]: #method.set
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, Table, json};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// // Has an `age` field
	/// table.insert(json!({ "first_name": "Hiruna", "age": 19 }), &connection)?;
	/// // Does not have an `age` field
	/// table.insert(json!({ "first_name": "Bob" }), &connection)?;
	/// // `insert` should only work for objects which don't have the field already
	/// table.iter().insert("age", 13, &connection);
	/// let people: Vec<(String, u8)> = table.iter().fields(&["first_name", "age"], &connection)?;
	/// assert_eq!(people[0], ("Hiruna".into(), 19));
	/// assert_eq!(people[1], ("Bob".into(), 13)); // Only Bob was changed
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn insert<T, C>(&self, field: &str, value: T, connection: C) -> SqliteResult<()>
	where
		T: ToSql,
		C: AsRef<SqliteConnection>,
	{
		let path = format_key(field);
		let set_value = format!("{} = json_insert({},\"{}\",:value)", self.data_key, self.data_key, path);
		connection.as_ref().execute_named(
			&format!("UPDATE {} SET {} {}", self.table_key, set_value, self.make_clauses()),
			&[(":value", &value)]
		).map(|_|())
	}

	/// Uses a JSON object update or create fields in the entry's JSON object.
	///
	/// Any fields that do not exist will be created.
	/// This unfortunately does not work with Arrays. It will replace Arrays instead.
	/// To insert into Arrays, use [`insert`] instead.
	///
	/// [`insert`]: #method.insert
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, json, Table};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// table.insert(json!({ "first_name": "Hiruna", "age": 19 }), &connection)?;
	/// table.insert(json!({ "first_name": "Bob" }), &connection)?;
	/// table.insert(json!({ "first_name": "Alex", "grades": [6, 8, 7] }), &connection)?;
	/// // `patch` overwrites any field
	/// // arrays are completely replace
	/// table.iter().patch(json!({ "age": 13, "grades": [9] }), &connection);
	/// let people: Vec<(u8, Vec<u8>)> = table.iter().fields(&["age", "grades"], &connection)?;
	/// for person in people.into_iter() {
	/// 	// `age` field was overwritten and set to 13
	/// 	assert_eq!(person.0, 13);
	/// 	// `grades` field was overwritten and set to an array of one element
	/// 	assert_eq!(person.1, [9])
	/// }
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn patch<T, C>(&self, value: T, connection: C) -> SqliteResult<()>
	where
		T: Serialize,
		C: AsRef<SqliteConnection>,
	{
		let set_value = format!("{} = json_patch({},:value)", self.data_key, self.data_key);
		connection.as_ref().execute_named(
			&format!("UPDATE {} SET {} {}", self.table_key, set_value, self.make_clauses()),
			&[(":value", &Json(value))]
		).map(|_|())
	}

	/// Replaces a field in a JSON object with a given value.
	///
	/// Will only replace an already existing field.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, Table, json};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// // Has an `age` field
	/// table.insert(json!({ "first_name": "Hiruna", "age": 19 }), &connection)?;
	/// // Does not have an `age` field
	/// table.insert(json!({ "first_name": "Bob" }), &connection)?;
	/// // `replace` should only work for objects which have the field already
	/// table.iter().replace("age", 13, &connection);
	/// let people: Vec<u8> = table.iter().field("age", &connection)?;
	/// // Both objects had their fields set
	/// assert_eq!(people.len(), 1);
	/// assert_eq!(people[0], 13);
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn replace<T, C>(&self, field: &str, value: T, connection: C) -> SqliteResult<()>
	where
		T: ToSql,
		C: AsRef<SqliteConnection>,
	{
		let path = format_key(field);
		let set_value = format!("{} = json_replace({},\"{}\",:value)", self.data_key, self.data_key, path);
		connection.as_ref().execute_named(
			&format!("UPDATE {} SET {} {}", self.table_key, set_value, self.make_clauses()),
			&[(":value", &value)]
		).map(|_|())
	}

	/// Sets a field in a JSON object to a given field.
	///
	/// If the field does not exist, it will be created.
	/// If the field does already exist, it will be overwritten.
	/// If you wish for the value to not be overwritten, use [`insert`] instead.
	///
	/// [`insert`]: #method.insert
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, Table, json};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// // Has an `age` field
	/// table.insert(json!({ "first_name": "Hiruna", "age": 19 }), &connection)?;
	/// // Does not have an `age` field
	/// table.insert(json!({ "first_name": "Bob" }), &connection)?;
	/// table.iter().set("age", 13, &connection);
	/// let people: Vec<u8> = table.iter().field("age", &connection)?;
	/// // Both objects had their fields set
	/// assert_eq!(people[0], 13);
	/// assert_eq!(people[1], 13);
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn set<T, C>(&self, field: &str, value: T, connection: C) -> SqliteResult<()>
	where
		T: ToSql,
		C: AsRef<SqliteConnection>,
	{
		let path = format_key(field);
		let set_value = format!("{} = json_set({},\"{}\",:value)", self.data_key, self.data_key, path);
		connection.as_ref().execute_named(
			&format!("UPDATE {} SET {} {}", self.table_key, set_value, self.make_clauses()),
			&[(":value", &value)]
		).map(|_|())
	}

	/// Applies a filter on what entries the command will operate on.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, field, Table, json};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// table.insert(json!({"name": "Hiruna", "age": 19}), &connection)?;
	/// table.insert(json!({"name": "Bob", "age": 13}), &connection)?;
	/// table.insert(json!({"name": "Callum", "age": 12}), &connection)?;
	/// table.insert(json!({"name": "John", "age": 20}), &connection)?;
	/// // We only want people 18 or above
	/// let people: Vec<(String, u8)> = table.iter()
	/// 	.filter(field("age").gte(18))
	/// 	.fields(&["name", "age"], &connection)?;
	/// // Only 2 people should have passed the filter
	/// assert_eq!(people.len(), 2);
	/// for person in people.into_iter() {
	/// 	// They should be 18+ years old
	/// 	assert!(person.1 >= 18);
	/// }
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn filter<A: Filter>(self, filter: A) -> Iterator<'a, I, A> {
		Iterator {
			where_: filter,
			id_key: self.id_key,
			id_type: self.id_type,
			limit: None,
			offset: self.offset,
			table_key: self.table_key,
			data_key: self.data_key,
		}
	}

	/// Skip over `n` entries.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, field, Table, json};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// table.insert(json!({"name": "Hiruna", "age": 19}), &connection)?;
	/// table.insert(json!({"name": "Bob", "age": 13}), &connection)?;
	/// table.insert(json!({"name": "Callum", "age": 12}), &connection)?;
	/// table.insert(json!({"name": "John", "age": 20}), &connection)?;
	/// // We want the people third or above in position within the table
	/// let people: Vec<String> = table.iter()
	/// 	.skip(2)
	/// 	.field("name", &connection)?;
	/// // Only 2 people should have passed the filter
	/// assert_eq!(people.len(), 2);
	/// assert_eq!(people[0], "Callum");
	/// assert_eq!(people[1], "John");
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn skip(mut self, n: u32) -> Self {
		self.offset = Some(n);
		self
	}

	/// Take only `n` entries.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, field, Table, json};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// table.insert(json!({"name": "Hiruna", "age": 19}), &connection)?;
	/// table.insert(json!({"name": "Bob", "age": 13}), &connection)?;
	/// table.insert(json!({"name": "Callum", "age": 12}), &connection)?;
	/// table.insert(json!({"name": "John", "age": 20}), &connection)?;
	/// // We only want the first two people
	/// let people: Vec<String> = table.iter()
	/// 	.take(2)
	/// 	.field("name", &connection)?;
	/// // Only 2 people should have passed the filter
	/// assert_eq!(people.len(), 2);
	/// assert_eq!(people[0], "Hiruna");
	/// assert_eq!(people[1], "Bob");
	/// # rusqlite::Result::Ok(())
	/// ```
	pub fn take(mut self, n: u32) -> Self {
		self.limit = Some(n);
		self
	}

	/// Execute a query using the given command (e.g. "SELECT data"),
	/// the given function to handle the output, and the connection to the database.
	///
	/// *It is not recommended to use this method.*
	pub fn execute_get<A, F, C>(&self, command: &str, execute: F, connection: C) -> SqliteResult<A>
		where
			F: FnOnce(Statement, Vec<(&str, &dyn ToSql)>) -> SqliteResult<A>,
			C: AsRef<SqliteConnection>,
	{
		let con = connection.as_ref().prepare(&format!("{} FROM {} {}", command, &self.table_key, self.make_clauses()))?;
		let params = vec![];
		execute(con, params)
	}

	fn make_clauses(&self) -> String {
		let where_ = self.where_.where_(&self).map(|w| format!("WHERE {}", w)).unwrap_or_default();
		let limit = if self.limit.is_none() && self.offset.is_none() { String::new() }
		else { format!("LIMIT {} OFFSET {}", self.limit.map(|i| i as i64).unwrap_or(-1), self.offset.unwrap_or(0)) };
		format!("{} {}", where_, limit)
	}
}

fn get_first_column<T, A, F>(map: F) -> impl Fn(Statement, Vec<(&str, &dyn ToSql)>) -> SqliteResult<Vec<T>>
where
	A: FromSql,
	F: Fn(A) -> T,
{
	move |mut statement, params| {
		Ok(statement.query_map_named(&params, |row| row.get(0))?
			.filter_map(Result::ok)
			.map(&map)
			.collect())
	}
}

fn no_map<T>(in_: T) -> T { in_ }
