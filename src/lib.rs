//! This crate implements a NoSQL-like API over SQLite using SQLite's Json1 extension.
#![warn(missing_docs)]

use rusqlite::{Connection as SqliteConnection, Error as SqliteError, NO_PARAMS, OptionalExtension,
				Result as SqliteResult, Statement,
				types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, Value, ValueRef}};
use serde::{Deserialize, de::DeserializeOwned, Serialize};
use serde_json::to_string;

use std::{marker::PhantomData, path::Path};

/// A connection the underlying sqlite database.
pub struct Connection {
	connection: SqliteConnection,
}
impl Connection {
	/// Opens a connection to a sqlite database.
	///
	/// Creates one if it doesn't exist.
	pub fn open<P: AsRef<Path>>(path: P) -> SqliteResult<Self> {
		Ok(Self { connection: SqliteConnection::open(path)? })
	}

	/// Gets a table in the database.
	///
	/// Creates one if it doesn't exist.
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
	pub fn iter(&self) -> Iterator<I, ()> {
		Iterator {
			data_key: &self.data,
			id_key: &self.id,
			id_type: self.id_type.clone(),
			limit: None,
			offset: None,
			table_key: &self.name,
			where_: (),
		}
	}

	/// Inserts a JSON object into the data column of the table.
	///
	/// **Warning**: If your table has other columns that are not nullable, then you should not use this.
	pub fn insert<T: Serialize, C: AsRef<SqliteConnection>>(&self, data: T, connection: C) -> SqliteResult<()> {
		connection.as_ref().prepare(&format!("INSERT INTO {} ({}) VALUES (?)", self.name, self.data))?
			.execute(&[&Json(data)])?;
		Ok(())
	}
}
impl <I: FromSql + ToSql> Table<I> {
	/// Gets a JSON object using a id from the id column.
	pub fn get(&self, id: I) -> Get<I> { Get { id, data_key: &self.data, id_key: &self.id, table: &self.name } }
}

fn format_key(key: &str) -> Option<String> {
	let mut chars = key.chars();
	let mut prepend = String::with_capacity(2+key.len());
	if chars.next() != Some('$') {
		prepend.push('$');
		let c = chars.next();
		if c != Some('.') && c.is_some() {
			prepend.push('.');
		}
	}
	prepend.push_str(key);
	Some(prepend)
}

/// A struct that represents AND.
///
/// How this is used depends on the situation.
pub struct And<A, B> {
	/// The first struct to be used.
	pub first: A,
	/// The second struct to be used.
	pub second: B,
}

/// A struct that represents equality.
pub struct Eq<A, B> {
	/// The variable that is being checked/set.
	pub variable: A,
	/// The value that is being checked for/set.
	pub value: B,
}

/// Represents an operation to get a JSON object using its id key.
#[must_use = "This struct must be used for the database to be queried."]
pub struct Get<'a, I: FromSql + ToSql> {
	data_key: &'a str,
	id: I,
	id_key: &'a str,
	table: &'a str,
}
impl<'a, I: FromSql + ToSql> Get<'a, I> {
	/// Gets only the JSON object, deserialising it into the struct provided.
	pub fn data<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Option<Json<T>>> {
		connection.as_ref().query_row(
			&format!("SELECT {} FROM {} WHERE {} = ?", self.data_key, self.table, self.id_key),
			&[&self.id],
			|row| row.get(0)
		).optional()
	}
	/// Gets both the id and the JSON object.
	pub fn entry<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Option<Entry<I, T>>> {
		connection.as_ref().query_row(
			&format!("SELECT {}, {} FROM {} WHERE {} = ?", self.id_key, self.data_key, self.table, self.id_key),
			&[&self.id],
			|row| Ok(Entry { id: row.get(0)?, data: row.get(1)? })
		).optional()
	}
	/// Gets only the id of the entry.
	pub fn id<C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Option<I>> {
		connection.as_ref().query_row(
			&format!("SELECT {} FROM {} WHERE {} = ?", self.data_key, self.table, self.id_key),
			&[&self.id],
			|row| row.get(0)
		).optional()
	}
	/// Extracts a possibly nested field in the JSON object.
	pub fn key<T: FromSql, C: AsRef<SqliteConnection>>(&self, key: &str, connection: C) -> SqliteResult<Option<T>> {
		let key = format_key(key).unwrap();
		connection.as_ref().query_row(
			&format!("SELECT json_extract({}, \"{}\") FROM {} WHERE {} = ?", self.data_key, key, self.table, self.id_key),
			&[&self.id],
			|row| row.get(0)
		).optional()
	}
}

fn get_first_column<T>(mut statement: Statement, params: Vec<(&str, &dyn ToSql)>) -> SqliteResult<Vec<T>>
where T: FromSql
{
	Ok(
		statement.query_map_named(
			&params,
			|row| row.get(0)
	)?.into_iter()
		.filter_map(|result| result.ok())
		.collect())
}

/// Represents a potential operation on a table.
#[must_use = "This struct does not do anything until executed"]
pub struct Iterator<'a, I, W> {
	data_key: &'a str,
	id_key: &'a str,
	id_type: PhantomData<fn() -> I>,
	limit: Option<u32>,
	offset: Option<u32>,
	where_: W,
	table_key: &'a str,
}
impl<'a, I: FromSql, W: Filter> Iterator<'a, I, W> {
	/// Execute a query using the given command (e.g. "SELECT data"),
	/// the given function to handle the output, and the connection to the database.
	pub fn execute<A, F, C>(&self, command: &str, execute: F, connection: C) -> SqliteResult<A>
	where
		F: FnOnce(Statement, Vec<(&str, &dyn ToSql)>) -> SqliteResult<A>,
		C: AsRef<SqliteConnection>,
	{
		let where_ = self.where_.where_(&self).map(|w| format!("WHERE {}", w)).unwrap_or_default();
		let limit = if self.limit.is_none() && self.offset.is_none() { String::new() }
			else { format!("LIMIT {} OFFSET {}", self.limit.map(|i| i as i64).unwrap_or(-1), self.offset.unwrap_or(0)) };
		let con = connection.as_ref().prepare(&format!("{} FROM {} {} {}", command, &self.table_key, where_, limit))?;
		let params = vec![];
		execute(con, params)
	}

	/// ***GET***s only the JSON object.
	pub fn data<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Vec<Json<T>>> {
		self.execute::<_, _, _>(
			&format!("SELECT {}", self.data_key),
			get_first_column,
			connection
		)
	}

	/// ***GET***s the id and the JSON object.
	pub fn entry<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Vec<Entry<I, T>>> {
		self.execute::<_, _, _>(
			&format!("SELECT {}, {}", self.id_key, self.data_key),
			|mut statement, params| {
				Ok(statement.query_map_named(
					&params,
					|row| Ok(Entry { id: row.get(0)?, data: row.get(1)? })
				)?.into_iter().filter_map(|result| result.ok()).collect::<Vec<_>>())
			},
			connection
		)
	}

	/// ***GET***s just the id of the entry.
	pub fn id<C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Vec<I>> {
		self.execute::<_, _, _>(
			&format!("SELECT {}", self.id_key),
			get_first_column,
			connection
		)
	}

	/// ***GET***s a field of the JSON object.
	pub fn field<T: FromSql, C: AsRef<SqliteConnection>>(&self, field_: &str, connection: C) -> SqliteResult<Vec<T>> {
		self.execute::<_, _, _>(
			&format!("SELECT {}", field(field_).key(&self)),
			get_first_column,
			connection
		)
	}

	/// ***GET***s multiple fields from the JSON object.
	pub fn fields<'b, F, T, C, S>(&self, fields: F, connection: C) -> SqliteResult<Vec<T>>
	where
		F: IntoIterator<Item=S>,
		S: AsRef<str>,
		T: DeserializeOwned,
		C: AsRef<SqliteConnection>,
	{
		let fields = fields.into_iter()
			.filter_map(|s| format_key(s.as_ref()))
			.fold(String::new(), |mut init, field| {
				init.push_str(",\"");
				init.push_str(field.as_str());
				init.push('"');
				init
			});
		self.execute::<_, _, _>(
			&format!("SELECT json_extract({}{})", self.data_key, fields),
			|mut statement, params| {
				Ok(statement.query_map_named(
					&params,
					|row| -> SqliteResult<Json<T>> { row.get(0) }
				)?.into_iter().filter_map(|result| result.ok()).map(Json::unwrap).collect::<Vec<_>>())
			},
			connection
		)
	}

	/// Applies a filter on what entries the command will operate on.
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
	pub fn skip(mut self, n: u32) -> Self {
		self.offset = Some(n);
		self
	}

	/// Take only `n` entries.
	pub fn take(mut self, n: u32) -> Self {
		self.limit = Some(n);
		self
	}
}

/// Represents a field in a JSON object.
pub struct Field(pub String);
/// Creates a representation of a field in a JSON object.
pub fn field(field: &str) -> Field { Field(format_key(field).unwrap()) }
impl Field {
	/// Takes in a value and serialises it, the serialised output is used in the eventual operation.
	pub fn eq<T: Serialize>(self, value: T) -> Eq<Field, String> {
		Eq { variable: self, value: to_string(&value).unwrap() }
	}
	fn key<'a, A, B>(&self, iter: &Iterator<'a, A, B>) -> String {
		format!("json_extract({}, \"{}\")", iter.data_key, self.0)
	}
}

/// Represents a condition which will determine what entries the operation can work on.
pub trait Filter {
	/// Returns a string formatted for use in an SQL statement.
	fn where_<'a, A, B>(&self, _: &Iterator<'a, A, B>) -> Option<String>;
	/// Allows chaining of multiple conditions.
	fn and<B: Filter>(self, second: B) -> And<Self, B>
		where Self: std::marker::Sized {
		And { first: self, second }
	}
}
impl Filter for () {
	fn where_<'a, A, B>(&self, _: &Iterator<'a, A, B>) -> Option<String> { None }
}
impl Filter for String {
	fn where_<'a, A, B>(&self, _: &Iterator<'a, A, B>) -> Option<String> { Some(self.clone()) }
}
impl<A: Filter, B: Filter> Filter for And<A, B> {
	fn where_<'a, C, D>(&self, iter: &Iterator<'a, C, D>) -> Option<String> {
		Some(format!("{} AND {}",
			self.first.where_(iter).unwrap_or_default(),
			self.second.where_(iter).unwrap_or_default()))
	}
}
impl Filter for Eq<Field, String> {
	fn where_<'a, A, B>(&self, iter: &Iterator<'a, A, B>) -> Option<String> {
		Some(format!("{} = {}", self.variable.key(iter), self.value))
	}
}

/// Represents one 'row' of the table.
#[derive(Debug, Deserialize)]
pub struct Entry<K, V> {
	/// The id of the entry.
	pub id: K,
	/// The JSON object.
	pub data: Json<V>,
}
impl<K, V> Entry<K, V> {
	/// Gets the JSON object out of the entry.
	pub fn data(&self) -> &V {
		&self.data.0
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
