use std::marker::PhantomData;

use rusqlite::{Connection as SqliteConnection, Result as SqliteResult, Statement,
	types::{FromSql, ToSql}};
use serde::{de::DeserializeOwned, Serialize};

use crate::{Entry, field, Filter, format_key, Json, Key, Sort};

/// Represents a potential operation on a table.
#[must_use = "This struct does not do anything until executed"]
pub struct Iterator<'a, I, W, S> {
	pub(crate) data_key: &'a str,
	pub(crate) id_key: &'a str,
	pub(crate) id_type: PhantomData<fn() -> I>,
	pub(crate) limit: Option<u32>,
	pub(crate) offset: Option<u32>,
	pub(crate) order_by: S,
	pub(crate) where_: W,
	pub(crate) table_key: &'a str,
}
impl<'a, I: FromSql, W: Filter, S: Sort> Iterator<'a, I, W, S> {
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
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	pub fn data<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Vec<T>> {
		self.execute::<_, _, _>(
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
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	pub fn entry<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Vec<Entry<I, T>>> {
		self.execute::<_, _, _>(
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
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	pub fn id<C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Vec<I>> {
		self.execute::<_, _, _>(
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
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	pub fn field<T: FromSql, C: AsRef<SqliteConnection>>(&self, field_: &str, connection: C) -> SqliteResult<Vec<T>> {
		self.execute::<_, _, _>(
			&format!("SELECT {}", field(field_).key(&self.data_key)),
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
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	pub fn fields<T, F, C, A>(&self, fields: F, connection: C) -> SqliteResult<Vec<T>>
	where
		F: IntoIterator<Item=A>,
		A: AsRef<str>,
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
		self.execute::<_, _, _>(
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
	/// # Ok::<(), rusqlite::Error>(())
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
	/// # Ok::<(), rusqlite::Error>(())

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

	/// Removes a *field* from a JSON object.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, Table, json};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("people")?;
	/// table.insert(json!({ "first_name": "Hiruna", "age": 19 }), &connection)?;
	/// table.insert(json!({ "first_name": "Bob", "age": 13 }), &connection)?;
	///
	/// table.iter().remove("age", &connection);
	/// // This should return no entries because no JSON object has an `age` field
	/// let people: Vec<u8> = table.iter().field("age", &connection)?;
	/// assert_eq!(people.len(), 0);
	/// // This *does not* delete the entries
	/// assert_eq!(table.iter().id(&connection)?.len(), 2);
	/// # Ok::<(), rusqlite::Error>(())

	/// ```
	pub fn remove<C>(&self, field: &str, connection: C) -> SqliteResult<()>
	where C: AsRef<SqliteConnection>
	{
		let path = format_key(field);
		let set_value = format!("{} = json_remove({}, '{}')", self.data_key, self.data_key, path);
		connection.as_ref().execute(
			&format!("UPDATE {} SET {} {}", self.table_key, set_value, self.make_clauses()),
			rusqlite::NO_PARAMS
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
	/// # Ok::<(), rusqlite::Error>(())

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
	/// # Ok::<(), rusqlite::Error>(())
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

	/// Deletes the entry.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, field, Key, Table, json};
	/// # use serde::{Deserialize, Serialize};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// // Add some random numbers to the table
	/// table.insert(9, &connection)?;
	/// table.insert(10, &connection)?;
	/// table.insert(12, &connection)?;
	/// table.insert(3, &connection)?;
	/// // We expect 4 entries
	/// let length = table.iter().id(&connection)?.len();
	/// assert_eq!(length, 4);
	/// // Now we'll remove every number 10 or above.
	/// table.iter().filter(field("").gte(10)).delete(&connection)?;
	/// // There should only be 2 entries left.
	/// let length = table.iter().id(&connection)?.len();
	/// assert_eq!(length, 2);
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn delete<C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<()> {
		self.execute("DELETE",
			|mut statement, params| statement.execute_named(&params),
			connection
		).map(|_|())
	}

	/// Applies a filter on what entries the command will operate on.
	///
	/// # Example
	///
	/// ```
	/// # use nosqlite::{Connection, Entry, field, Key, Table, json};
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
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn filter<A: Filter>(self, filter: A) -> Iterator<'a, I, A, S> {
		Iterator {
			where_: filter,
			id_key: self.id_key,
			id_type: self.id_type,
			limit: self.limit,
			offset: self.offset,
			order_by: self.order_by,
			table_key: self.table_key,
			data_key: self.data_key,
		}
	}

	/// Sort by using a specific field(s)
	///
	/// # Examples
	///
	/// ```
	/// # use nosqlite::{Connection, field, json, Key, Table};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"a": 3, "b": 2}), &connection)?;
	/// table.insert(json!({"a": 6, "b": 1}), &connection)?;
	/// table.insert(json!({"a": 2, "b": 8}), &connection)?;
	/// table.insert(json!({"a": 8, "b": 4}), &connection)?;
	///
	/// let data: Vec<u8> = table.iter().sort(field("a").ascending()).field("a", &connection)?;
	///
	/// assert_eq!(data[0], 2);
	/// assert_eq!(data[1], 3);
	/// assert_eq!(data[2], 6);
	/// assert_eq!(data[3], 8);
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	///
	/// ```
	/// # use nosqlite::{Connection, field, Key, json, Sort, Table};
	/// # let connection = Connection::in_memory()?;
	/// # let table = connection.table("test")?;
	/// table.insert(json!({"a": 2, "b": 2}), &connection)?;
	/// table.insert(json!({"a": 2, "b": 1}), &connection)?;
	/// table.insert(json!({"a": 8, "b": 4}), &connection)?;
	/// table.insert(json!({"a": 2, "b": 8}), &connection)?;
	///
	/// let data: Vec<(u8, u8)> = table.iter()
	/// 	.sort(field("a").ascending().and(field("b").ascending()))
	/// 	.fields(&["a","b"], &connection)?;
	///
	/// assert!(data[0].0 == 2 && data[0].1 == 1);
	/// assert!(data[1].0 == 2 && data[1].1 == 2);
	/// assert!(data[2].0 == 2 && data[2].1 == 8);
	/// assert!(data[3].0 == 8 && data[3].1 == 4);
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn sort<A: Sort>(self, key: A) -> Iterator<'a, I, W, A> {
		Iterator {
			data_key: self.data_key,
			where_: self.where_,
			id_key: self.id_key,
			id_type: self.id_type,
			limit: self.limit,
			offset: self.offset,
			order_by: key,
			table_key: self.table_key,
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
	/// # Ok::<(), rusqlite::Error>(())
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
	/// # Ok::<(), rusqlite::Error>(())
	/// ```
	pub fn take(mut self, n: u32) -> Self {
		self.limit = Some(n);
		self
	}

	/// Execute a query using the given command (e.g. "SELECT data"),
	/// the given function to handle the output, and the connection to the database.
	///
	/// *It is not recommended to use this method.*
	pub fn execute<A, F, C>(&self, command: &str, execute: F, connection: C) -> SqliteResult<A>
		where
			F: FnOnce(Statement, Vec<(&str, &dyn ToSql)>) -> SqliteResult<A>,
			C: AsRef<SqliteConnection>,
	{
		let con = connection.as_ref().prepare(&format!("{} FROM {} {}", command, &self.table_key, self.make_clauses()))?;
		let params = vec![];
		execute(con, params)
	}

	fn make_clauses(&self) -> String {
		let where_ = self.where_.where_(&self.data_key).map(|w| format!("WHERE {}", w)).unwrap_or_default();
		let limit = if self.limit.is_none() && self.offset.is_none() { String::new() }
		else { format!("LIMIT {} OFFSET {}", self.limit.map(|i| i as i64).unwrap_or(-1), self.offset.unwrap_or(0)) };
		let order = self.order_by.order_by(&self.data_key);
		let order = if order.is_empty() { String::new() } else {
			let mut first_time = true;
			order.into_iter()
				.fold("ORDER BY ".to_string(), |mut string, key| {
					if !first_time {
						string.push(',');
					}
					first_time = false;
					string.push_str(&key);
					string
				})
		};
		format!("{} {} {}", where_, limit, order)
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
