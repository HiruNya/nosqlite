//! This crate implements a NoSQL-like API over SQLite using SQLite's Json1 extension.
//!
//! If this is your first time here, start off by reading the [`Connection`] docs.
//!
//! [`Connection`]: struct.Connection.html
#![warn(missing_docs)]
#![allow(clippy::tabs_in_doc_comments)]

use rusqlite::{Connection as SqliteConnection, Error as SqliteError, NO_PARAMS,
				Result as SqliteResult, Row,
				types::{FromSqlError, FromSqlResult, ToSqlOutput, Value, ValueRef}};
use serde::{Deserialize, de::DeserializeOwned, Serialize};
use serde_json::to_string;

use std::{marker::{PhantomData, Sized}, path::Path};

mod iterator;
pub use iterator::Iterator;
mod key;
pub use key::{column, Column, field, Field, format_key, Key};
mod table;
pub use table::{Operation, Table};
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
