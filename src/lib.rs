//! This crate implements a NoSQL-like API over SQLite using SQLite's Json1 extension.
#![warn(missing_docs)]

use rusqlite::{Connection as SqliteConnection, Error as SqliteError, NO_PARAMS, OptionalExtension,
				Result as SqliteResult,
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
	pub fn entry<T: DeserializeOwned, C: AsRef<SqliteConnection>>(&self, connection: C) -> SqliteResult<Option<Entry<T, I>>> {
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

/// Represents one 'row' of the table.
#[derive(Debug, Deserialize)]
pub struct Entry<V, K> {
	/// The id of the entry.
	pub id: K,
	/// The JSON object.
	pub data: Json<V>,
}
impl<V, K> Entry<V, K> {
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
