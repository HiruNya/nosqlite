use std::marker::PhantomData;

use rusqlite::{Connection as SqliteConnection, Result as SqliteResult, Statement, types::{FromSql, ToSql}};
use serde::de::DeserializeOwned;

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
